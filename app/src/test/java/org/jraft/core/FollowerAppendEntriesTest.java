package org.jraft.core;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.jraft.rpc.AppendEntriesRequest;
import org.jraft.rpc.LogEntry;
import org.jraft.rpc.NodeId;
import org.jraft.state.LogStore;
import org.jraft.state.RaftState;
import org.junit.jupiter.api.Test;

public class FollowerAppendEntriesTest {

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

  static final class NoOpFsm implements FiniteStateMachine {
    public ApplyResult apply(LogEntry e) {
      return new ApplyResult(0, true, new byte[]{}, false);
    }
  }

  @Test
  void rejectsOlderTerm() {
    var s = new TdState(); s.setCurrentTerm(5);
    var log = new MemLog();
    var req = AppendEntriesRequest.newBuilder()
        .setTerm(4).setLeaderId(NodeId.newBuilder().setId("L"))
        .setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build();

    var resp = FollowerHandlers.onAppendEntries(new NoOpFsm(), log, s, req);
    
    assertFalse(resp.getSuccess());
    assertEquals(5, resp.getTerm());
  }

  @Test
  void acceptsHeartbeatOnPrevMatch_andAdvancesCommit() {
    var s = new TdState(); s.setCurrentTerm(5);
    var log = new MemLog();
    for (int i=1;i<=5;i++) log.add(i,2);

    var req = AppendEntriesRequest.newBuilder()
        .setTerm(5).setLeaderId(NodeId.newBuilder().setId("L"))
        .setPrevLogIndex(5).setPrevLogTerm(2)
        .setLeaderCommit(4) // heartbeat
        .build();

    var resp = FollowerHandlers.onAppendEntries(new NoOpFsm(),log, s, req);
    assertTrue(resp.getSuccess());
    assertEquals(4, s.getCommitIndex());
  }

  @Test
  void truncatesOnConflict_thenAppends() {
    var s = new TdState(); s.setCurrentTerm(7);
    var log = new MemLog();
    log.add(1,1); log.add(2,1); log.add(3,2); log.add(4,2);
    log.add(5,3); log.add(6,3); log.add(7,3);

    var req = AppendEntriesRequest.newBuilder()
        .setTerm(7).setLeaderId(NodeId.newBuilder().setId("L"))
        .setPrevLogIndex(4).setPrevLogTerm(2)
        .addEntries(LogEntry.newBuilder().setIndex(5).setTerm(4))
        .addEntries(LogEntry.newBuilder().setIndex(6).setTerm(4))
        .setLeaderCommit(6)
        .build();

    var resp = FollowerHandlers.onAppendEntries(new NoOpFsm(),log, s, req);
    assertTrue(resp.getSuccess());
    assertEquals(6, s.getCommitIndex());
    assertEquals(6, log.lastIndex());
    assertEquals(4, log.termAt(5));
    assertEquals(4, log.termAt(6));
  }

  @Test
  void rejectsOnPrevMismatch() {
    var s = new TdState(); s.setCurrentTerm(5);
    var log = new MemLog();
    for (int i=1;i<=5;i++) log.add(i,2);

    var req = AppendEntriesRequest.newBuilder()
        .setTerm(5).setLeaderId(NodeId.newBuilder().setId("L"))
        .setPrevLogIndex(5).setPrevLogTerm(9) // mismatch
        .addAllEntries(java.util.List.of())
        .setLeaderCommit(5).build();

    var resp = FollowerHandlers.onAppendEntries(new NoOpFsm(),log, s, req);
    assertFalse(resp.getSuccess());
    assertEquals(5, log.lastIndex()); // unchanged
    assertEquals(2, log.termAt(5));
  }

  
}
