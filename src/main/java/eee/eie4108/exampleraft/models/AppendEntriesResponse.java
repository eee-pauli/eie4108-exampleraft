package eee.eie4108.exampleraft.models;

public class AppendEntriesResponse {
  private int term;
  private boolean success;
  
  public AppendEntriesResponse() {}
  public AppendEntriesResponse(int term, boolean success) {
    this.term = term;
    this.success = success;
  }
  // Getters and Setters
  public int getTerm() { return term; }
  public void setTerm(int term) { this.term = term; }
  public boolean isSuccess() { return success; }
  public void setSuccess(boolean success) { this.success = success; }
}
