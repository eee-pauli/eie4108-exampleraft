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

// State-of-art implementation of Raft Consensus Algorithm
// Enhancement from RaftNode (V1):
// DO replicate log to on-line nodes AND backtracking and check for log consistency
@Path("/node")
public class RaftNodeV2 {
  
  // --- Basic Node State ---
  private final String nodeId;
  private final List<String> peerUrls; // Full URLs: "http://localhost:8081"
  private final ReentrantLock lock = new ReentrantLock();
  
  // --- Raft State (volatile for visibility across threads) ---
  private volatile NodeState state = NodeState.FOLLOWER;
  private volatile int currentTerm = 0;
  private volatile String votedFor = null;
  private final List<LogEntry> log = new ArrayList<>(); // log index starts at 1
  private final Map<String, String> stateMachine = new ConcurrentHashMap<>();
  
  // --- Volatile Raft indices ---
  private volatile int commitIndex = 0;
  private volatile int lastApplied = 0;
  
  // --- Leader-specific state ---
  private volatile String currentLeaderId = null;
  // Map of peerUrl -> next log index to send to that peer
  private Map<String, Integer> nextIndex;
  // Map of peerUrl -> the highest log index known to be replicated on peer
  private Map<String, Integer> matchIndex;
  
  // --- Timers and Executors ---
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
  private final Client client = ClientBuilder.newClient();
  private long lastHeartbeatTime;
  private static final int ELECTION_TIMEOUT_MS = 2000 + new Random().nextInt(1500); // Randomized timeout
  private static final int HEARTBEAT_INTERVAL_MS = 500; // This is now the replication interval
  
  public RaftNodeV2() {
    System.out.println("RaftNode must be initialized using RaftNode(String nodeId, List<String> peerUrls)");
    throw new IllegalArgumentException("Missing nodeId, peerUrls");
  }
  
  public RaftNodeV2(String nodeId, List<String> peerUrls) {
    this.nodeId = nodeId;
    this.peerUrls = peerUrls;
    System.out.printf("RaftNodeV2 started with nodeId: %s\n", nodeId);
    System.out.printf("Node %s created. Election timeout: %dms%n", nodeId, ELECTION_TIMEOUT_MS);
    
    // Add a dummy entry at index 0 to simplify log indexing (Raft logs are 1-indexed)
    log.add(new LogEntry(0, ""));
  }
  
  public void start() {
    // Start the election timer thread
    resetElectionTimer();
    scheduler.scheduleAtFixedRate(this::checkElectionTimeout, 0, ELECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    // Start the "apply committed entries" task
    scheduler.scheduleAtFixedRate(this::applyCommittedEntries, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }
  
  // --- JAX-RS Endpoints for Raft RPCs ---
  
  @POST
  @Path("/requestVote")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public VoteResponse requestVote(VoteRequest request) {
    lock.lock();
    try {
      // System.out.printf("Node %s received vote request from %s for term %d%n", nodeId, request.getCandidateId(), request.getTerm());
      
      // Rule 1: Reply false if term < currentTerm
      if (request.getTerm() < currentTerm) {
        return new VoteResponse(currentTerm, false);
      }
      
      // If we see a higher term, we become a follower
      if (request.getTerm() > currentTerm) {
        stepDown(request.getTerm());
      }
      
      // Rule 2: If votedFor is null or candidateId, and candidate's log is at least as up-to-date
      boolean voteAvailable = (votedFor == null || votedFor.equals(request.getCandidateId()));
      
      if (voteAvailable && isCandidateLogUpToDate(request)) {
        votedFor = request.getCandidateId();
        resetElectionTimer(); // Granting a vote resets the timer
        System.out.printf("Node %s voted for %s in term %d%n", nodeId, request.getCandidateId(), currentTerm);
        return new VoteResponse(currentTerm, true);
      } else {
        return new VoteResponse(currentTerm, false);
      }
      
    } finally {
      lock.unlock();
    }
  }
  
  /**
   * Checks if the candidate's log is "at least as up-to-date" as this node's log.
   * Raft determines this by comparing the index and term of the last entries in the logs.
   */
  private boolean isCandidateLogUpToDate(VoteRequest request) {
    int lastLogTerm = getLastLogTerm();
    int lastLogIndex = getLastLogIndex();
    
    // Raft safety rule:
    if (request.getLastLogTerm() > lastLogTerm) {
      return true; // Candidate's log has a higher term
    }
    if (request.getLastLogTerm() == lastLogTerm && request.getLastLogIndex() >= lastLogIndex) {
      return true; // Same term, but candidate's log is longer or equal
    }
    return false; // This node's log is more up-to-date
  }
  
  @POST
  @Path("/appendEntries")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public AppendEntriesResponse appendEntries(AppendEntriesRequestV2 request) {
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
      
      // Rule 2: Reply false if log doesn't contain an entry at prevLogIndex
      // whose term matches prevLogTerm
      if (request.getPrevLogIndex() > getLastLogIndex() ||
              log.get(request.getPrevLogIndex()).getTerm() != request.getPrevLogTerm()) {
        System.out.printf("Node %s rejecting AppendEntries from %s. Log mismatch. MyLast: %d, ReqPrev: %d%n",
                          nodeId, request.getLeaderId(), getLastLogIndex(), request.getPrevLogIndex());
        return new AppendEntriesResponse(currentTerm, false);
      }
      
      // --- Log consistency check passed ---
      
      // Rule 3: If an existing entry conflicts with a new one (same index, different terms),
      // delete the existing entry and all that follow it.
      List<LogEntry> newEntries = request.getEntries();
      if (newEntries != null && !newEntries.isEmpty()) {
        System.out.printf("Node %s received %d entries from leader %s%n", nodeId, newEntries.size(), request.getLeaderId());
        int index = request.getPrevLogIndex() + 1;
        for (LogEntry entry : newEntries) {
          if (index <= getLastLogIndex()) {
            if (log.get(index).getTerm() != entry.getTerm()) {
              // Conflict found. Truncate log from this point.
              System.out.printf("Node %s: Log conflict at index %d. Truncating log.%n", nodeId, index);
              log.subList(index, log.size()).clear();
              log.add(entry);
            }
            // else: entry already matches, do nothing
          } else {
            // No conflict, just append
            log.add(entry);
          }
          index++;
        }
      } else {
        // It's a heartbeat
        // System.out.printf("Node %s received heartbeat from leader %s in term %s%n", nodeId, request.getLeaderId(), currentTerm);
      }
      
      // Rule 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
      if (request.getLeaderCommit() > commitIndex) {
        commitIndex = Math.min(request.getLeaderCommit(), getLastLogIndex());
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
      
      // The entry is just added to the leader's log.
      // The 'replicateLogsToPeers' job will replicate it.
      // In a real system, the client would wait (or poll) for commit.
      // For this demo, we'll just return "accepted by leader".
      return new ClientResponse(true, "Command accepted by leader " + nodeId, nodeId);
      
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
  
  private int getLastLogIndex() {
    return log.size() - 1;
  }
  
  private int getLastLogTerm() {
    if (getLastLogIndex() == 0) return 0; // Term of dummy entry is 0
    return log.get(getLastLogIndex()).getTerm();
  }
  
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
    
    final int termOfElection = currentTerm;
    final int lastLogIdx = getLastLogIndex();
    final int lastLogTrm = getLastLogTerm();
    
    // Count self-vote
    long votesReceived = 1;
    
    // Ask for votes from peers
    for (String peerUrl : peerUrls) {
      try {
        WebTarget target = client.target(peerUrl).path("/node/requestVote");
        VoteRequest request = new VoteRequest(termOfElection, nodeId, lastLogIdx, lastLogTrm);
        Response response = target.request(MediaType.APPLICATION_JSON).post(Entity.json(request));
        if (response.getStatus() == 200) {
          VoteResponse voteResponse = response.readEntity(VoteResponse.class);
          if (voteResponse.isVoteGranted()) {
            votesReceived++;
          } else if (voteResponse.getTerm() > currentTerm) {
            // Found a node with a higher term, step down
            stepDown(voteResponse.getTerm());
            return;
          }
        }
      } catch (Exception e) {
        System.err.printf("Node %s unable to request vote from %s%n", nodeId, peerUrl);
      }
    }
    
    // Check if we won
    // Total number of nodes = peerUrls.size() + 1 (self)
    int totalNodes = peerUrls.size() + 1;
    int majority = (totalNodes / 2) + 1;
    System.out.printf("Node %s get %d/%d votes from peer%n", nodeId, votesReceived, totalNodes);
    if (state == NodeState.CANDIDATE && votesReceived >= majority) {
      becomeLeader();
    }
  }
  
  /**
   * This method is called periodically by the leader to replicate logs to followers.
   * It also serves as the heartbeat mechanism.
   */
  private void replicateLogsToPeers() {
    if (state != NodeState.LEADER) return;
    
    // First, update the leader's own commit index based on follower matchIndex
    updateLeaderCommitIndex();
    
    // Then, send AppendEntries to all peers
    for (String peerUrl : peerUrls) {
      try {
        int next = nextIndex.get(peerUrl);
        int lastLogIdx = getLastLogIndex();
        
        // Calculate prevLogIndex based on this specific follower's progress
        int prevLogIndex = next - 1;
        int prevLogTerm = log.get(prevLogIndex).getTerm();
        
        // If we have logs to send, send them; otherwise, it's an empty heartbeat
        List<LogEntry> entriesToSend = (lastLogIdx >= next)
                                           ? new ArrayList<>(log.subList(next, lastLogIdx + 1))
                                           : Collections.emptyList();
        
        AppendEntriesRequestV2 request = new AppendEntriesRequestV2(currentTerm, nodeId, prevLogIndex, prevLogTerm, entriesToSend, commitIndex);
        WebTarget target = client.target(peerUrl).path("/node/appendEntries");
        
        // IMPORTANT: Process the response even for heartbeats
        Response response = target.request(MediaType.APPLICATION_JSON).post(Entity.json(request));
        
        if (response.getStatus() == 200) {
          AppendEntriesResponse r = response.readEntity(AppendEntriesResponse.class);
          if (r.isSuccess()) {
            // Success: node is now up to at least the index we sent
            nextIndex.put(peerUrl, Math.max(next, lastLogIdx + 1));
            matchIndex.put(peerUrl, Math.max(matchIndex.get(peerUrl), lastLogIdx));
          } else {
            if (r.getTerm() > currentTerm) {
              stepDown(r.getTerm());
              return;
            }
            // Consistency check failed: decrement nextIndex and try again in next interval
            System.out.printf("Leader %s: Peer %s rejected index %d. Backtracking.%n", nodeId, peerUrl, prevLogIndex);
            nextIndex.put(peerUrl, Math.max(1, next - 1));
          }
        }
      } catch (Exception e) {
        // System.err.printf("Leader %s failed to replicate to %s: %s%n", nodeId, peerUrl, e.getMessage());
      }
    }
  }
  
  /**
   * Leader-only function to update its commitIndex based on follower acknowledgments.
   */
  private void updateLeaderCommitIndex() {
    // Find the highest log index replicated on a majority of servers
    int totalNodes = peerUrls.size() + 1;
    int majority = (totalNodes / 2) + 1;
    
    for (int N = getLastLogIndex(); N > commitIndex; N--) {
      // Check if entry at index N is from the current term
      if (log.get(N).getTerm() == currentTerm) {
        // Count how many nodes (including self) have this entry
        int replicas = 1; // Start with self
        for (String peerUrl : peerUrls) {
          if (matchIndex.get(peerUrl) >= N) {
            replicas++;
          }
        }
        
        if (replicas >= majority) {
          System.out.printf("Leader %s committing log index %d%n", nodeId, N);
          commitIndex = N;
          break; // Per Raft: only commit entries from the current term
        }
      }
    }
  }
  
  /**
   * This runs on all nodes, applying committed entries (from lastApplied up to commitIndex)
   * to the state machine.
   */
  private void applyCommittedEntries() {
    lock.lock();
    try {
      while (lastApplied < commitIndex) {
        lastApplied++;
        LogEntry entry = log.get(lastApplied);
        applyLogEntry(entry);
      }
    } finally {
      lock.unlock();
    }
  }
  
  
  private void becomeLeader() {
    lock.lock();
    try {
      if (state != NodeState.CANDIDATE) return; // Lost the race
      
      System.out.printf("** Node %s is becoming LEADER for term %d **%n", nodeId, currentTerm);
      state = NodeState.LEADER;
      currentLeaderId = nodeId;
      
      // Initialize leader state
      nextIndex = new ConcurrentHashMap<>();
      matchIndex = new ConcurrentHashMap<>();
      int lastLogIdx = getLastLogIndex();
      for (String peerUrl : peerUrls) {
        nextIndex.put(peerUrl, lastLogIdx + 1);
        matchIndex.put(peerUrl, 0);
      }
      
      // Stop old tasks if any, and start the new one
      // (Scheduler handles this automatically if we just re-schedule)
      scheduler.scheduleAtFixedRate(this::replicateLogsToPeers, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    } finally {
      lock.unlock();
    }
  }
  
  private void stepDown(int newTerm) {
    lock.lock();
    try {
      // Only update term if it's newer
      if (newTerm > currentTerm) {
        currentTerm = newTerm;
      }
      state = NodeState.FOLLOWER;
      votedFor = null;
      currentLeaderId = null;
      // Stop leader-specific tasks
      // In this simple model, the 'replicateLogsToPeers' task will
      // just check 'if (state != NodeState.LEADER) return;' and do nothing.
      System.out.printf("Node %s stepping down to follower in term %d%n", nodeId, currentTerm);
    } finally {
      lock.unlock();
    }
  }
  
  private void resetElectionTimer() {
    lastHeartbeatTime = System.currentTimeMillis();
  }
  
  /**
   * Applies a single log entry to the state machine.
   */
  private void applyLogEntry(LogEntry entry) {
    String[] parts = entry.getCommand().split("\\s+");
    if (parts.length == 3 && "SET".equalsIgnoreCase(parts[0])) {
      stateMachine.put(parts[1], parts[2]);
      System.out.printf("Node %s applied command [Idx: %d, Term: %d]: %s. State: %s%n",
                        nodeId, lastApplied, entry.getTerm(), entry.getCommand(), stateMachine);
    }
  }
}
