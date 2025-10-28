package eee.eie4108.exampleraft.models;

public class VoteResponse {
  private int term;
  private boolean voteGranted;
  
  public VoteResponse() {}
  public VoteResponse(int term, boolean voteGranted) {
    this.term = term;
    this.voteGranted = voteGranted;
  }
  // Getters and Setters
  public int getTerm() { return term; }
  public void setTerm(int term) { this.term = term; }
  public boolean isVoteGranted() { return voteGranted; }
  public void setVoteGranted(boolean voteGranted) { this.voteGranted = voteGranted; }
}
