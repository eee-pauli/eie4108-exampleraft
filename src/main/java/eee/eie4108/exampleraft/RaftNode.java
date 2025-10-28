package eee.eie4108.exampleraft;

import eee.eie4108.exampleraft.models.*;
import jakarta.ws.rs.*;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

// Simplified implementation of Raft Consensus Algorithm
// Only replicate log to on-line nodes WITHOUT backtracking and check for log consistency
@Path("/node")
public class RaftNode {
  
  // --- Basic Node State ---
  private final String nodeId;
  private final List<String> peerUrls;
  private final ReentrantLock lock = new ReentrantLock();
  
  // --- Raft State (volatile for visibility across threads) ---
  private volatile NodeState state = NodeState.FOLLOWER;
  private volatile int currentTerm = 0;
  private volatile String votedFor = null;
  private final List<LogEntry> log = new ArrayList<>();
  private final Map<String, String> stateMachine = new ConcurrentHashMap<>();
  
  // --- Leader-specific state ---
  private volatile String currentLeaderId = null;
  
  // --- Timers and Executors ---
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
  private final Client client = ClientBuilder.newClient();
  private long lastHeartbeatTime;
  private static final int ELECTION_TIMEOUT_MS = 2000 + new Random().nextInt(1500); // Randomized timeout
  private static final int HEARTBEAT_INTERVAL_MS = 500;
  
  public RaftNode() {
    System.out.println("RaftNode must be initialized using RaftNode(String nodeId, List<String> peerUrls)");
    throw new IllegalArgumentException("Missing nodeId, peerUrls");
  }
  
  public RaftNode(String nodeId, List<String> peerUrls) {
    this.nodeId = nodeId;
    this.peerUrls = peerUrls;
    System.out.printf("RaftNode started with nodeId: %s\n", nodeId);
    System.out.printf("Node %s created. Election timeout: %dms%n", nodeId, ELECTION_TIMEOUT_MS);
  }
  
  public void start() {
    // Start the election timer thread
    resetElectionTimer();
    scheduler.scheduleAtFixedRate(this::checkElectionTimeout, 0, ELECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }
  
  // --- JAX-RS Endpoints for Raft RPCs ---
  
  @POST
  @Path("/requestVote")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public VoteResponse requestVote(VoteRequest request) {
    lock.lock();
    try {
      System.out.printf("Node %s received vote request from %s for term %d%n", nodeId, request.getCandidateId(), request.getTerm());
      
      // Rule 1: Reply false if term < currentTerm
      if (request.getTerm() < currentTerm) {
        return new VoteResponse(currentTerm, false);
      }
      
      // If we see a higher term, we become a follower
      if (request.getTerm() > currentTerm) {
        stepDown(request.getTerm());
      }
      
      // Rule 2: If votedFor is null or candidateId, and candidate's log is at least as up-to-date, grant vote
      boolean alreadyVoted = votedFor != null && !votedFor.equals(request.getCandidateId());
      if (alreadyVoted) {
        return new VoteResponse(currentTerm, false);
      }
      
      // Simplified log up-to-date check. Real Raft is more complex.
      // Here, we just ensure we haven't already voted in this term for someone else.
      votedFor = request.getCandidateId();
      System.out.printf("Node %s voted for %s in term %d%n", nodeId, request.getCandidateId(), currentTerm);
      return new VoteResponse(currentTerm, true);
      
    } finally {
      lock.unlock();
    }
  }
  
  @POST
  @Path("/appendEntries")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
    lock.lock();
    try {
      // Rule 1: Reply false if term < currentTerm
      if (request.getTerm() < currentTerm) {
        return new AppendEntriesResponse(currentTerm, false);
      }
      
      // We have found the leader or a new leader
      resetElectionTimer();
      if (request.getTerm() > currentTerm || state == NodeState.CANDIDATE) {
        stepDown(request.getTerm());
      }
      
      currentLeaderId = request.getLeaderId();
      
      // This is a heartbeat if entries are empty
      if (request.getEntries() == null || request.getEntries().isEmpty()) {
        // System.out.printf("Node %s received heartbeat from leader %s in term %s%n", nodeId, request.getLeaderId(), currentTerm);
      } else {
        System.out.printf("Node %s received entries from leader %s%n", nodeId, request.getLeaderId());
        for (LogEntry entry : request.getEntries()) {
          log.add(entry);
          // In a real system, you'd apply this to the state machine once committed
          applyLogEntry(entry);
        }
      }
      return new AppendEntriesResponse(currentTerm, true);
    } finally {
      lock.unlock();
    }
  }
  
  @POST
  @Path("/client")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public ClientResponse handleClientRequest(ClientRequest request) {
    if (state != NodeState.LEADER) {
      System.out.printf("Node %s (not leader) redirecting client to %s%n", nodeId, currentLeaderId);
      return new ClientResponse(false, "Not the leader. Current leader is " + currentLeaderId, currentLeaderId);
    }
    
    lock.lock();
    try {
      System.out.printf("Leader %s received client request: %s%n", nodeId, request.getCommand());
      LogEntry newEntry = new LogEntry(currentTerm, request.getCommand());
      log.add(newEntry);
      
      // Replicate to peers
      int successfulReplications = replicateLog();
      
      // Simplified: commit immediately if replicated to a majority
      if (successfulReplications + 1 > peerUrls.size() / 2) {
        applyLogEntry(newEntry);
        return new ClientResponse(true, "Command processed by leader " + nodeId, nodeId);
      } else {
        return new ClientResponse(false, "Failed to replicate command to a majority.", nodeId);
      }
    } finally {
      lock.unlock();
    }
  }
  @GET
  @Path("/get/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response handleClientGet(@PathParam("key") String key) {
    // This is a simplified read. It reads from the local state machine.
    // In a real Raft implementation, this could return stale data if this
    // follower is partitioned from the leader.
    // For strict linearizability, reads must also go through the leader,
    // or the follower must confirm its state with the leader (read-lease).
    
    System.out.printf("Node %s received GET request for key: %s%n", nodeId, key);
    String value = stateMachine.get(key);
    
    if (value != null) {
      Map<String, String> responseData = new HashMap<>();
      responseData.put("key", key);
      responseData.put("value", value);
      return Response.ok(responseData).build();
    } else {
      Map<String, String> responseData = new HashMap<>();
      responseData.put("message", "Key not found");
      return Response.status(Response.Status.NOT_FOUND).entity(responseData).build();
    }
  }
  
  
  // --- Internal Raft Logic ---
  
  private void checkElectionTimeout() {
    if (state != NodeState.LEADER && (System.currentTimeMillis() - lastHeartbeatTime) > ELECTION_TIMEOUT_MS) {
      System.out.printf("Node %s timed out, starting election for term %d...%n", nodeId, currentTerm + 1);
      startElection();
    }
  }
  
  
  private void startElection() {
    lock.lock();
    try {
      currentTerm++;
      state = NodeState.CANDIDATE;
      votedFor = nodeId; // Vote for self
      resetElectionTimer();
    } finally {
      lock.unlock();
    }
    
    // Use a final variable for the lambda
    final int termOfElection = currentTerm;
    
    // Ask for votes from peers
    long votesReceived = peerUrls.stream()
                                 .map(peerUrl -> {
                                   try {
                                     System.out.printf("Node %s is requesting votes from %s%n", nodeId, peerUrl);
                                     WebTarget target = client.target(peerUrl).path("/node/requestVote");
                                     VoteRequest request = new VoteRequest(termOfElection, nodeId, log.size(), 0); // Simplified log info
                                     Response response = target.request(MediaType.APPLICATION_JSON).post(Entity.json(request));
                                     if (response.getStatus() == 200) {
                                       return response.readEntity(VoteResponse.class);
                                     }
                                   } catch (Exception e) {
                                     System.err.printf("Node %s unable to request vote from %s%n", nodeId, peerUrl);
                                   }
                                   return new VoteResponse(0, false);
                                 })
                                 .filter(VoteResponse::isVoteGranted)
                                 .count();
    
    // Check if we won (1 vote for self + votes from peers)
    System.out.printf("Node %s get %d/%d votes from peer%n", nodeId, votesReceived + 1, peerUrls.size() + 1);
    if (state == NodeState.CANDIDATE && (votesReceived + 1) > peerUrls.size() / 2) {
      becomeLeader();
    }
  }
  
  private int replicateLog() {
    int successfulReplications = 0;
    for (String peerUrl : peerUrls) {
      if (peerUrl.contains(nodeId)) continue; // Don't send to self
      try {
        WebTarget target = client.target(peerUrl).path("/node/appendEntries");
        // Simplified: send the last entry. A real implementation sends entries based on follower's state.
        // TODO log backtracking
        AppendEntriesRequest request = new AppendEntriesRequest(currentTerm, nodeId, List.of(log.get(log.size()-1)));
        Response response = target.request(MediaType.APPLICATION_JSON).post(Entity.json(request));
        
        if (response.getStatus() == 200 && response.readEntity(AppendEntriesResponse.class).isSuccess()) {
          successfulReplications++;
        }
      } catch (Exception e) {
        // System.err.printf("Leader %s failed to replicate log to %s: %s%n", nodeId, peerUrl, e.getMessage());
      }
    }
    return successfulReplications;
  }
  
  private void becomeLeader() {
    lock.lock();
    try {
      if (state != NodeState.CANDIDATE) return; // Lost the race
      
      System.out.printf("** Node %s is becoming LEADER for term %d**%n", nodeId, currentTerm);
      state = NodeState.LEADER;
      currentLeaderId = nodeId;
      
      // Start sending heartbeats
      scheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    } finally {
      lock.unlock();
    }
  }
  
  private void sendHeartbeats() {
    if (state != NodeState.LEADER) return;
    
    // System.out.printf("Leader %s sending heartbeats for term %d%n", nodeId, currentTerm);
    
    for (String peerUrl : peerUrls) {
      if (peerUrl.contains(nodeId)) continue;
      try {
        WebTarget target = client.target(peerUrl).path("/node/appendEntries");
        // Send out dummy AppendEntriesRequest to other nodes
        AppendEntriesRequest heartbeat = new AppendEntriesRequest(currentTerm, nodeId, Collections.emptyList());
        target.request(MediaType.APPLICATION_JSON).post(Entity.json(heartbeat));
      } catch (Exception e) {
        // Ignore, node might be down. Will retry.
      }
    }
  }
  
  private void stepDown(int newTerm) {
    currentTerm = newTerm;
    state = NodeState.FOLLOWER;
    votedFor = null;
    System.out.printf("Node %s stepping down to follower in term %d%n", nodeId, newTerm);
  }
  
  private void resetElectionTimer() {
    lastHeartbeatTime = System.currentTimeMillis();
  }
  
  private void applyLogEntry(LogEntry entry) {
    String[] parts = entry.getCommand().split("\\s+");
    if (parts.length == 3 && "SET".equalsIgnoreCase(parts[0])) {
      stateMachine.put(parts[1], parts[2]);
      System.out.printf("Node %s applied command: %s. State: %s%n", nodeId, entry.getCommand(), stateMachine);
    }
  }
}

