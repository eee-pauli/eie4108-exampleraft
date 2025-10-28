package eee.eie4108.exampleraft.models;

import java.util.List;

public class AppendEntriesRequestV2 {
  private int term;
  private String leaderId;
  private int prevLogIndex;
  private int prevLogTerm;
  private List<LogEntry> entries;
  private int leaderCommit;
  
  // No-arg constructor for JSON deserialization
  public AppendEntriesRequestV2() {}
  
  public AppendEntriesRequestV2(int term, String leaderId, int prevLogIndex, int prevLogTerm, List<LogEntry> entries, int leaderCommit) {
    this.term = term;
    this.leaderId = leaderId;
    this.prevLogIndex = prevLogIndex;
    this.prevLogTerm = prevLogTerm;
    this.entries = entries;
    this.leaderCommit = leaderCommit;
  }
  
  // Getters
  public int getTerm() {return term;}
  
  public String getLeaderId() {return leaderId;}
  
  public int getPrevLogIndex() {return prevLogIndex;}
  
  public int getPrevLogTerm() {return prevLogTerm;}
  
  public List<LogEntry> getEntries() {return entries;}
  
  public int getLeaderCommit() {return leaderCommit;}
  
  // Setters are needed for JSON deserialization
  public void setTerm(int term) {this.term = term;}
  
  public void setLeaderId(String leaderId) {this.leaderId = leaderId;}
  
  public void setPrevLogIndex(int prevLogIndex) {this.prevLogIndex = prevLogIndex;}
  
  public void setPrevLogTerm(int prevLogTerm) {this.prevLogTerm = prevLogTerm;}
  
  public void setEntries(List<LogEntry> entries) {this.entries = entries;}
  
  public void setLeaderCommit(int leaderCommit) {this.leaderCommit = leaderCommit;}
}