package eee.eie4108.exampleraft.models;

public class ClientResponse {
  private boolean success;
  private String message;
  private String leaderId;
  
  public ClientResponse() {}
  public ClientResponse(boolean success, String message, String leaderId) {
    this.success = success;
    this.message = message;
    this.leaderId = leaderId;
  }
  // Getters and Setters
  public boolean isSuccess() { return success; }
  public void setSuccess(boolean success) { this.success = success; }
  public String getMessage() { return message; }
  public void setMessage(String message) { this.message = message; }
  public String getLeaderId() { return leaderId; }
  public void setLeaderId(String leaderId) { this.leaderId = leaderId; }
}
