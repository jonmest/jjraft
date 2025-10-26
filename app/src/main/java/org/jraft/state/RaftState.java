package org.jraft.state;


public class RaftState {
  protected long currentTerm = 0;
  public enum Role { FOLLOWER, CANDIDATE, LEADER; }
  protected String votedFor = null;
  protected long commitIndex = 0;
  protected long lastApplied = 0;

  protected Role role = Role.FOLLOWER;
  protected String leaderId = null;

  public long getCurrentTerm() { return this.currentTerm; }
  public void setCurrentTerm(long term) { this.currentTerm = term; }

  public long getLastApplied() { return this.lastApplied; }
  public void setLastApplied(long applied) { this.lastApplied = applied; }
  
  public String getVotedFor() { return this.votedFor; }
  public void setVotedFor(String votedFor) { this.votedFor = votedFor; }
  public void clearVote() { this.votedFor = null; }

  public void becomeFollower() { 
    this.role = Role.FOLLOWER; 
    this.leaderId = null;
  }
  public void becomeCandidate() { 
    this.role = Role.CANDIDATE;
    this.leaderId = null; 
  }
  
  public void becomeLeader() { 
    this.role = Role.LEADER;
 }

  /**
   * Set the role. Subclasses can override to mirror the role into their own fields.
   */
  public void setRole(Role r) { this.role = r; }

  public long getCommitIndex() { return this.commitIndex; }
  public void setCommitIndex(long index) { this.commitIndex = index; }

  public String getLeader() { return this.leaderId; }
  public void setLeader(String leaderId) { this.leaderId = leaderId; }

  public Role getRole() { return this.role; }
}
