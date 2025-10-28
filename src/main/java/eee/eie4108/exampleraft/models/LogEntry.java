package eee.eie4108.exampleraft.models;

public class LogEntry {
  private int term;
  private String command;
  
  public LogEntry() {} // For JSON deserialization
  
  public LogEntry(int term, String command) {
    this.term = term;
    this.command = command;
  }
  // Getters and Setters
  public int getTerm() { return term; }
  public void setTerm(int term) { this.term = term; }
  public String getCommand() { return command; }
  public void setCommand(String command) { this.command = command; }
}
