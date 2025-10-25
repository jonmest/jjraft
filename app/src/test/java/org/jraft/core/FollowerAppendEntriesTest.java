package org.jraft.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.jraft.rpc.AppendEntriesRequest;
import org.jraft.rpc.LogEntry;
import org.jraft.rpc.NodeId;
import org.jraft.test.TestHelpers.MemLog;
import org.jraft.test.TestHelpers.TestRaftState;
import org.jraft.test.TestHelpers.TrackingStateMachine;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

public class FollowerAppendEntriesTest {

  @Test
  void rejectsOlderTerm() {
    var state = new TestRaftState();
    state.setCurrentTerm(5);
    var log = new MemLog();
    var fsm = new TrackingStateMachine();

    var req = AppendEntriesRequest.newBuilder()
        .setTerm(4)
        .setLeaderId(NodeId.newBuilder().setId("L"))
        .setPrevLogIndex(0)
        .setPrevLogTerm(0)
        .setLeaderCommit(0)
        .build();

    var resp = FollowerHandlers.onAppendEntries(fsm, log, state, req);

    assertFalse(resp.getSuccess(), "should reject older term");
    assertEquals(5, resp.getTerm());
    assertEquals(0, fsm.appliedEntries.size(), "no entries should be applied");
  }

  @Test
  void acceptsHeartbeatOnPrevMatch_andAdvancesCommit() {
    var state = new TestRaftState();
    state.setCurrentTerm(5);
    var log = new MemLog();
    var fsm = new TrackingStateMachine();

    // Add entries to log
    for (int i = 1; i <= 5; i++) {
      log.add(i, 2, ByteString.copyFromUtf8("data" + i));
    }

    var req = AppendEntriesRequest.newBuilder()
        .setTerm(5)
        .setLeaderId(NodeId.newBuilder().setId("L"))
        .setPrevLogIndex(5)
        .setPrevLogTerm(2)
        .setLeaderCommit(4) // commit up to index 4
        .build();

    var resp = FollowerHandlers.onAppendEntries(fsm, log, state, req);

    assertTrue(resp.getSuccess(), "heartbeat should succeed");
    assertEquals(4, state.getCommitIndex(), "commit index should advance");
    assertEquals(4, state.getLastApplied(), "should apply committed entries");
    assertEquals(4, fsm.appliedEntries.size(), "should apply 4 entries to FSM");

    // Verify entries were applied in order
    for (int i = 0; i < 4; i++) {
      assertEquals(i + 1, fsm.appliedEntries.get(i).getIndex());
    }
  }

  @Test
  void truncatesOnConflict_thenAppends_andAppliesCommitted() {
    var state = new TestRaftState();
    state.setCurrentTerm(7);
    var log = new MemLog();
    var fsm = new TrackingStateMachine();

    // Old log: [1:1, 2:1, 3:2, 4:2, 5:3, 6:3, 7:3]
    log.add(1, 1);
    log.add(2, 1);
    log.add(3, 2);
    log.add(4, 2);
    log.add(5, 3);
    log.add(6, 3);
    log.add(7, 3);

    // Leader sends new entries from index 5 with different term
    var req = AppendEntriesRequest.newBuilder()
        .setTerm(7)
        .setLeaderId(NodeId.newBuilder().setId("L"))
        .setPrevLogIndex(4)
        .setPrevLogTerm(2)
        .addEntries(LogEntry.newBuilder().setIndex(5).setTerm(4).setData(ByteString.copyFromUtf8("new5")))
        .addEntries(LogEntry.newBuilder().setIndex(6).setTerm(4).setData(ByteString.copyFromUtf8("new6")))
        .setLeaderCommit(6)
        .build();

    var resp = FollowerHandlers.onAppendEntries(fsm, log, state, req);

    assertTrue(resp.getSuccess(), "should succeed");
    assertEquals(6, state.getCommitIndex(), "commit index should advance to 6");
    assertEquals(6, log.lastIndex(), "log should have 6 entries (7 was truncated)");
    assertEquals(4, log.termAt(5), "index 5 should have new term");
    assertEquals(4, log.termAt(6), "index 6 should have new term");

    // Verify FSM application
    assertEquals(6, state.getLastApplied(), "should apply all committed entries");
    assertEquals(6, fsm.appliedEntries.size(), "should apply 6 entries to FSM");
    assertEquals(ByteString.copyFromUtf8("new5"), fsm.appliedEntries.get(4).getData());
    assertEquals(ByteString.copyFromUtf8("new6"), fsm.appliedEntries.get(5).getData());
  }

  @Test
  void rejectsOnPrevMismatch_noLogModification() {
    var state = new TestRaftState();
    state.setCurrentTerm(5);
    var log = new MemLog();
    var fsm = new TrackingStateMachine();

    for (int i = 1; i <= 5; i++) {
      log.add(i, 2);
    }

    var req = AppendEntriesRequest.newBuilder()
        .setTerm(5)
        .setLeaderId(NodeId.newBuilder().setId("L"))
        .setPrevLogIndex(5)
        .setPrevLogTerm(9) // mismatch!
        .setLeaderCommit(5)
        .build();

    var resp = FollowerHandlers.onAppendEntries(fsm, log, state, req);

    assertFalse(resp.getSuccess(), "should reject on prev mismatch");
    assertEquals(5, log.lastIndex(), "log should be unchanged");
    assertEquals(2, log.termAt(5), "term should be unchanged");
    assertEquals(0, state.getCommitIndex(), "commit should not advance");
    assertEquals(0, fsm.appliedEntries.size(), "no entries should be applied");
  }

  @Test
  void onlyAppliesUpToMinOfCommitAndLogLength() {
    var state = new TestRaftState();
    state.setCurrentTerm(5);
    var log = new MemLog();
    var fsm = new TrackingStateMachine();

    // Log has 3 entries
    log.add(1, 2);
    log.add(2, 2);
    log.add(3, 2);

    // Leader says commit is 10 (but we only have 3)
    var req = AppendEntriesRequest.newBuilder()
        .setTerm(5)
        .setLeaderId(NodeId.newBuilder().setId("L"))
        .setPrevLogIndex(3)
        .setPrevLogTerm(2)
        .setLeaderCommit(10) // way ahead
        .build();

    var resp = FollowerHandlers.onAppendEntries(fsm, log, state, req);

    assertTrue(resp.getSuccess());
    assertEquals(3, state.getCommitIndex(), "should only commit up to log length");
    assertEquals(3, state.getLastApplied());
    assertEquals(3, fsm.appliedEntries.size());
  }

  @Test
  void doesNotReapplyAlreadyAppliedEntries() {
    var state = new TestRaftState();
    state.setCurrentTerm(5);
    state.setCommitIndex(2);
    state.setLastApplied(2);
    var log = new MemLog();
    var fsm = new TrackingStateMachine();

    for (int i = 1; i <= 5; i++) {
      log.add(i, 2);
    }

    // Leader advances commit to 4
    var req = AppendEntriesRequest.newBuilder()
        .setTerm(5)
        .setLeaderId(NodeId.newBuilder().setId("L"))
        .setPrevLogIndex(5)
        .setPrevLogTerm(2)
        .setLeaderCommit(4)
        .build();

    var resp = FollowerHandlers.onAppendEntries(fsm, log, state, req);

    assertTrue(resp.getSuccess());
    assertEquals(4, state.getCommitIndex());
    assertEquals(4, state.getLastApplied());
    // Should only apply indices 3 and 4 (not 1 and 2, already applied)
    assertEquals(2, fsm.appliedEntries.size());
    assertEquals(3, fsm.appliedEntries.get(0).getIndex());
    assertEquals(4, fsm.appliedEntries.get(1).getIndex());
  }
}
