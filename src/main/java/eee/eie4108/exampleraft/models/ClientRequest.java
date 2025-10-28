package eee.eie4108.exampleraft.models;

public class ClientRequest {
  private String command;
  public ClientRequest() {}
  public ClientRequest(String command) { this.command = command; }
  public String getCommand() { return command; }
  public void setCommand(String command) { this.command = command; }
}
