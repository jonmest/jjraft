package org.jraft.state;

import java.util.Map;


public class RaftState {
  long currentTerm = 0;
  public enum Role { FOLLOWER, CANDIDATE, LEADER; }
  String votedFor = null;
  //final LogStore log = new InMemoryLog();
  long commitIndex = 0;
  long lastApplied = 0;

  Map<String, Long> nextIndex = Map.of();   // followerId -> next index to send
  Map<String, Long> matchIndex = Map.of();  // followerId -> highest known replicated index

  Role role = Role.FOLLOWER;
  String leaderId = null;

  public long getCurrentTerm() { return this.currentTerm; }
  public void setCurrentTerm(long term) { this.currentTerm = term; }

  public long getLastApplied() { return this.lastApplied; }
  public void setLastApplied(long applied) { this.lastApplied = applied; }
  
  public String getVotedFor() { return this.votedFor; }
  public void setVotedFor(String votedFor) { this.votedFor = votedFor; }
  public void clearVote() { this.votedFor = null; }

  public void becomeFollower() { this.role = Role.FOLLOWER; }
  public void becomeCandidate() { this.role = Role.CANDIDATE; }
  public void becomeLeader() { this.role = Role.LEADER; }

  public long getCommitIndex() { return this.commitIndex; }
  public void setCommitIndex(long index) { this.commitIndex = index; }

  public String getLeader() { return this.leaderId; }
  public void setLeader(String leaderId) { this.leaderId = leaderId; }
}
