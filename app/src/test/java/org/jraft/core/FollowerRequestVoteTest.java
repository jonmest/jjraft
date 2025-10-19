package org.jraft.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.jraft.rpc.NodeId;
import org.jraft.rpc.RequestVoteRequest;
import org.jraft.state.LogStore;
import org.jraft.state.RaftState;
import org.junit.jupiter.api.Test;

public class FollowerRequestVoteTest {

  static final class TdState extends RaftState {
    private long currentTerm, commitIndex, lastApplied;
    private String votedFor, leaderId; private Role role = Role.FOLLOWER;

    public long getCurrentTerm(){ return currentTerm; }
    public void setCurrentTerm(long t){ currentTerm = t; }
    public String getVotedFor(){ return votedFor; }
    public void setVotedFor(String id){ votedFor = id; }
    public void clearVote(){ votedFor = null; }
    public void becomeFollower(){ role = Role.FOLLOWER; }
    public long getCommitIndex(){ return commitIndex; }
    public void setCommitIndex(long i){ commitIndex = i; }
    public long getLastApplied(){ return lastApplied; }
    public void setLastApplied(long i){ lastApplied = i; }
    public void setLeader(String id){ leaderId = id; }
  }

  // Minimal in-memory log
  static final class MemLog implements LogStore {
    final java.util.ArrayList<org.jraft.rpc.LogEntry> a = new java.util.ArrayList<>();
    public long lastIndex(){ return a.size(); }
    public long termAt(long idx){ return (idx<=0 || idx>a.size())?0:a.get((int)idx-1).getTerm(); }
    public void append(java.util.List<org.jraft.rpc.LogEntry> batch){ if(!batch.isEmpty()){ if(batch.get(0).getIndex()!=lastIndex()+1) throw new IllegalStateException(); a.addAll(batch);} }
    public void truncateFrom(long idx){ while(a.size() >= idx && idx>0) a.remove(a.size()-1); }
    public org.jraft.rpc.LogEntry entryAt(long idx){ return (idx<=0||idx>a.size())?null:a.get((int)idx-1); }
    void add(long i,long t){ a.add(org.jraft.rpc.LogEntry.newBuilder().setIndex(i).setTerm(t).build()); }
  }

  @Test
  void rejectsOlderTerm() {
    var s = new TdState(); s.setCurrentTerm(5);
    var log = new MemLog();

    var req = RequestVoteRequest.newBuilder()
        .setTerm(4).setCandidateId(NodeId.newBuilder().setId("A"))
        .setLastLogIndex(0).setLastLogTerm(0).build();

    var resp = FollowerHandlers.onRequestVote(log, s, req);
    assertFalse(resp.getVoteGranted());
    assertEquals(5, resp.getTerm());
    assertNull(s.getVotedFor());
  }

  @Test
  void bumpsTermAndClearsVoteOnHigherTerm_thenDecides() {
    var s = new TdState(); s.setCurrentTerm(5); s.setVotedFor("X"); // stale vote
    var log = new MemLog();

    var req = RequestVoteRequest.newBuilder()
        .setTerm(6).setCandidateId(NodeId.newBuilder().setId("A"))
        .setLastLogIndex(0).setLastLogTerm(0).build();

    var resp = FollowerHandlers.onRequestVote(log, s, req);
    assertEquals(6, s.getCurrentTerm());
    assertTrue(resp.getVoteGranted());
    assertEquals("A", s.getVotedFor());
  }

  @Test
  void grantsAtMostOncePerTerm_butIdempotentForSameCandidate() {
    var s = new TdState(); s.setCurrentTerm(7);
    var log = new MemLog();

    // First RV from A (up-to-date)
    var reqA = RequestVoteRequest.newBuilder()
        .setTerm(7).setCandidateId(NodeId.newBuilder().setId("A"))
        .setLastLogIndex(0).setLastLogTerm(0).build();
    var r1 = FollowerHandlers.onRequestVote(log, s, reqA);
    assertTrue(r1.getVoteGranted());
    var r2 = FollowerHandlers.onRequestVote(log, s, reqA);
    assertTrue(r2.getVoteGranted());
    assertEquals("A", s.getVotedFor());

    var reqB = RequestVoteRequest.newBuilder()
        .setTerm(7).setCandidateId(NodeId.newBuilder().setId("B"))
        .setLastLogIndex(0).setLastLogTerm(0).build();
    var r3 = FollowerHandlers.onRequestVote(log, s, reqB);
    assertFalse(r3.getVoteGranted());
    assertEquals("A", s.getVotedFor());
  }

  @Test
  void enforcesUpToDateRule_termThenIndex() {
    var s = new TdState(); s.setCurrentTerm(10);
    var log = new MemLog();
    for (int i=1;i<=10;i++) log.add(i,3);

    var bad = RequestVoteRequest.newBuilder()
        .setTerm(10).setCandidateId(NodeId.newBuilder().setId("A"))
        .setLastLogIndex(9).setLastLogTerm(3).build();
    assertFalse(FollowerHandlers.onRequestVote(log,s,bad).getVoteGranted());

    var good = RequestVoteRequest.newBuilder()
        .setTerm(11).setCandidateId(NodeId.newBuilder().setId("B"))
        .setLastLogIndex(1).setLastLogTerm(4).build();
    assertTrue(FollowerHandlers.onRequestVote(log,s,good).getVoteGranted());
    assertEquals(11, s.getCurrentTerm());
    assertEquals("B", s.getVotedFor());
  }
  
}
