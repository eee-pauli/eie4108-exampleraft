package eee.eie4108.exampleraft.models;

import java.util.List;

public class AppendEntriesRequest {
  private int term;
  private String leaderId;
  private List<LogEntry> entries;
  
  public AppendEntriesRequest() {}
  public AppendEntriesRequest(int term, String leaderId, List<LogEntry> entries) {
    this.term = term;
    this.leaderId = leaderId;
    this.entries = entries;
  }
  // Getters and Setters
  public int getTerm() { return term; }
  public void setTerm(int term) { this.term = term; }
  public String getLeaderId() { return leaderId; }
  public void setLeaderId(String leaderId) { this.leaderId = leaderId; }
  public List<LogEntry> getEntries() { return entries; }
  public void setEntries(List<LogEntry> entries) { this.entries = entries; }
}

