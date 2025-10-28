package eee.eie4108.exampleraft.models;

public class VoteRequest {
  private int term;
  private String candidateId;
  private int lastLogIndex;
  private int lastLogTerm;
  
  public VoteRequest() {}
  public VoteRequest(int term, String candidateId, int lastLogIndex, int lastLogTerm) {
    this.term = term;
    this.candidateId = candidateId;
    this.lastLogIndex = lastLogIndex;
    this.lastLogTerm = lastLogTerm;
  }
  // Getters and Setters
  public int getTerm() { return term; }
  public void setTerm(int term) { this.term = term; }
  public String getCandidateId() { return candidateId; }
  public void setCandidateId(String candidateId) { this.candidateId = candidateId; }
  public int getLastLogIndex() { return lastLogIndex; }
  public void setLastLogIndex(int lastLogIndex) { this.lastLogIndex = lastLogIndex; }
  public int getLastLogTerm() { return lastLogTerm; }
  public void setLastLogTerm(int lastLogTerm) { this.lastLogTerm = lastLogTerm; }
}
