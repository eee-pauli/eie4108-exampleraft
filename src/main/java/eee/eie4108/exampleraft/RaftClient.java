package eee.eie4108.exampleraft;

import eee.eie4108.exampleraft.models.ClientRequest;
import eee.eie4108.exampleraft.models.ClientResponse;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RaftClient {
  public static void main(String[] args) throws InterruptedException {
    if (args.length < 2) {
      System.err.println("Usage: java RaftClient \"<command>\" <port1> <port2> ...");
      System.err.println("Example (Write): java RaftClient \"SET course 7031\" 8080 8081 8082");
      System.err.println("Example (Read):  java RaftClient \"GET course\" 8080 8081 8082");
      System.exit(1);
    }
    
    String commandStr = args[0];
    List<String> ports = Arrays.asList(args).subList(1, args.length);
    String randomNodeUrl = "http://localhost:" + ports.get(new Random().nextInt(ports.size()));
    
    Client client = ClientBuilder.newClient();
    int retries = 5;
    
    if (commandStr.toUpperCase().startsWith("SET")) {
      handleWriteCommand(client, commandStr, ports, randomNodeUrl, retries);
    } else if (commandStr.toUpperCase().startsWith("GET")) {
      handleReadCommand(client, commandStr, ports, randomNodeUrl, retries);
    } else {
      System.err.println("Unknown command. Must start with SET or GET.");
    }
  }
  
  private static void handleWriteCommand(Client client, String command, List<String> ports, String initialUrl, int maxRetries) throws InterruptedException {
    String leaderUrl = initialUrl;
    
    for (int i = 0; i < maxRetries; i++) {
      System.out.printf("Attempt %d: Sending WRITE command '%s' to %s%n", (i + 1), command, leaderUrl);
      try {
        WebTarget target = client.target(leaderUrl).path("/node/client");
        ClientRequest request = new ClientRequest(command);
        Response response = target.request(MediaType.APPLICATION_JSON).post(Entity.json(request));
        
        if (response.getStatus() == 200) {
          ClientResponse clientResponse = response.readEntity(ClientResponse.class);
          if (clientResponse.isSuccess()) {
            System.out.println("Replied: " + clientResponse.getMessage());
            return; // Done
          } else {
            // We hit a follower, it should tell us who the leader is
            System.out.println("Request failed: " + clientResponse.getMessage());
            String leaderId = clientResponse.getLeaderId();
            if (leaderId != null && leaderId.startsWith("node-")) {
              String leaderPort = leaderId.substring(5);
              leaderUrl = "http://localhost:" + leaderPort;
              System.out.println("Redirecting to new leader: " + leaderUrl);
            } else {
              // No leader known yet, try another random node after a delay
              leaderUrl = "http://localhost:" + ports.get(new Random().nextInt(ports.size()));
              Thread.sleep(1000);
            }
          }
        } else {
          System.err.println("HTTP Error: " + response.getStatus());
          leaderUrl = "http://localhost:" + ports.get(new Random().nextInt(ports.size()));
          Thread.sleep(1000);
        }
      } catch (Exception e) {
        System.err.println("Error connecting to " + leaderUrl + ": " + e.getMessage());
        // Try another random node
        leaderUrl = "http://localhost:" + ports.get(new Random().nextInt(ports.size()));
        Thread.sleep(2000);
      }
    }
    System.err.println("Failed to process WRITE command after multiple retries.");
  }
  
  private static void handleReadCommand(Client client, String command, List<String> ports, String initialUrl, int maxRetries) throws InterruptedException {
    String[] parts = command.split("\\s+");
    if (parts.length != 2) {
      System.err.println("Invalid GET command. Format: GET <key>");
      return;
    }
    String key = parts[1];
    String nodeUrl = initialUrl;
    
    for (int i = 0; i < maxRetries; i++) {
      System.out.printf("Attempt %d: Sending READ command for key '%s' to %s%n", (i + 1), key, nodeUrl);
      try {
        WebTarget target = client.target(nodeUrl).path("/node/get").path(key);
        Response response = target.request(MediaType.APPLICATION_JSON).get();
        
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
          // We use Map.class to easily deserialize the simple JSON object
          Map<String, String> responseData = response.readEntity(new GenericType<Map<String, String>>() {});
          System.out.printf("Replied: %s = %s%n", responseData.get("key"), responseData.get("value"));
          return; // Done
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
          System.out.println("Key not found.");
          return; // Done
        } else {
          System.err.println("HTTP Error: " + response.getStatus() + " from " + nodeUrl);
        }
      } catch (Exception e) {
        System.err.println("Error connecting to " + nodeUrl + ": " + e.getMessage());
      }
      
      // If we're here, the request failed. Try another random node.
      nodeUrl = "http://localhost:" + ports.get(new Random().nextInt(ports.size()));
      Thread.sleep(1000);
    }
    System.err.println("Failed to process READ command after multiple retries.");
  }
}
