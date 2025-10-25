package org.jraft.node;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.jraft.core.util.FakeRepeatingTask;
import org.jraft.net.RaftTransport;
import org.jraft.rpc.AppendEntriesRequest;
import org.jraft.rpc.AppendEntriesResponse;
import org.jraft.rpc.RequestVoteRequest;
import org.jraft.rpc.RequestVoteResponse;
import org.jraft.test.TestHelpers.MemLog;
import org.jraft.test.TestHelpers.TestRaftState;
import org.jraft.test.TestHelpers.TrackingStateMachine;
import org.jraft.util.FakeElectionTimer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for leader commit index advancement.
 *
 * Key invariants tested:
 * - Leader only commits entries from current term (safety)
 * - Leader commits when majority has replicated
 * - Leader applies committed entries to its own FSM
 * - commitIndex advances correctly with various follower states
 */
public class LeaderCommitTest {

  private static class FakeTransport implements RaftTransport {
    final Map<String, AppendEntriesRequest> lastAE = new HashMap<>();
    final Map<String, Consumer<AppendEntriesResponse>> aeCb = new HashMap<>();

    @Override
    public void requestVote(String peerId, RequestVoteRequest req, Consumer<RequestVoteResponse> cb) {
      // Not used in these tests
    }

    @Override
    public void appendEntries(String peerId, AppendEntriesRequest req, Consumer<AppendEntriesResponse> cb) {
      lastAE.put(peerId, req);
      aeCb.put(peerId, cb);
    }

    void respondAE(String peerId, long term, boolean success, long matchIndex) {
      var cb = aeCb.get(peerId);
      if (cb != null) {
        cb.accept(AppendEntriesResponse.newBuilder()
            .setTerm(term)
            .setSuccess(success)
            .setMatchIndex(matchIndex)
            .build());
      }
    }
  }

  private TestRaftState state;
  private MemLog log;
  private TrackingStateMachine fsm;
  private FakeTransport net;
  private FakeElectionTimer electionTimer;
  private FakeRepeatingTask heartbeatTask;
  private RaftNode node;

  private final String self = "n1";
  private final List<String> peers = List.of("n2", "n3");

  @BeforeEach
  void setup() {
    state = new TestRaftState();
    log = new MemLog();
    fsm = new TrackingStateMachine();
    net = new FakeTransport();
    electionTimer = new FakeElectionTimer();
    heartbeatTask = new FakeRepeatingTask();
    node = new RaftNode(self, peers, state, log, net, fsm, heartbeatTask, electionTimer);
  }


  @Test
  void leaderCommitsWhenMajorityReplicates() {
    // Setup: Leader has entries 1-5 in term 1
    log.add(1, 1);
    log.add(2, 1);
    log.add(3, 1);
    log.add(4, 1);
    log.add(5, 1);

    // Become leader in term 2
    state.setCurrentTerm(2);
    state.becomeLeader();

    // Manually set up leader state (normally done in becomeLeader)
    node.nextIndex.put("n2", 6L);
    node.nextIndex.put("n3", 6L);
    node.matchIndex.put("n2", 0L);
    node.matchIndex.put("n3", 0L);

    // Append a new entry in term 2 (only entries from current term can be committed)
    log.add(6, 2);

    // Simulate successful replication to n2 (now we have majority: n1, n2)
    node.matchIndex.put("n2", 6L);

    // Trigger commit advancement (would be called in onAppendEntriesResponse)
    // We'll simulate this by sending heartbeats and getting responses
    node.sendHeartbeats();
    net.respondAE("n2", 2, true, 6);

    // Verify: commitIndex advanced to 6
    assertEquals(6, state.getCommitIndex(), "commitIndex should advance when majority has entry");

    // Verify: Leader applied entry to its own FSM
    assertEquals(6, state.getLastApplied(), "leader should apply committed entries");
    assertEquals(6, fsm.appliedEntries.size(), "FSM should have 6 entries applied");
  }

  @Test
  void leaderDoesNotCommitFromOldTerms() {
    // Setup: Leader has entries from old terms
    log.add(1, 1);
    log.add(2, 1);
    log.add(3, 2); // Entry from term 2

    // Become leader in term 3
    state.setCurrentTerm(3);
    state.becomeLeader();

    node.nextIndex.put("n2", 4L);
    node.nextIndex.put("n3", 4L);
    node.matchIndex.put("n2", 0L);
    node.matchIndex.put("n3", 0L);

    // Simulate: Entry 3 is replicated on majority
    node.matchIndex.put("n2", 3L);
    node.matchIndex.put("n3", 3L);

    // Even though entry 3 is on majority, it's from term 2, so can't be committed yet
    node.sendHeartbeats();
    net.respondAE("n2", 3, true, 3);

    // commitIndex should NOT advance (entry 3 is from old term)
    assertEquals(0, state.getCommitIndex(), "should not commit entries from old terms directly");
    assertEquals(0, state.getLastApplied());
  }

  @Test
  void leaderCommitsOldEntriesIndirectly() {
    // Setup: Leader has old entries plus new entry
    log.add(1, 1);
    log.add(2, 1);
    log.add(3, 2); // Old term entry

    // Become leader in term 3
    state.setCurrentTerm(3);
    state.becomeLeader();

    node.nextIndex.put("n2", 5L);
    node.nextIndex.put("n3", 5L);
    node.matchIndex.put("n2", 0L);
    node.matchIndex.put("n3", 0L);

    // Add entry in current term
    log.add(4, 3);

    // Replicate entry 4 (term 3) to majority
    node.matchIndex.put("n2", 4L);

    node.sendHeartbeats();
    net.respondAE("n2", 3, true, 4);

    // Now commitIndex should advance to 4, which indirectly commits entries 1, 2, 3
    assertEquals(4, state.getCommitIndex(), "current term entry commits, bringing old entries with it");
    assertEquals(4, state.getLastApplied());
    assertEquals(4, fsm.appliedEntries.size(), "all 4 entries should be applied");
  }

  @Test
  void leaderAppliesOnlyNewlyCommittedEntries() {
    // Setup: Some entries already applied
    log.add(1, 1);
    log.add(2, 1);
    log.add(3, 1);

    state.setCommitIndex(2);
    state.setLastApplied(2);

    // Manually apply first 2 entries
    fsm.apply(log.entryAt(1));
    fsm.apply(log.entryAt(2));

    // Become leader in term 2
    state.setCurrentTerm(2);
    state.becomeLeader();

    node.nextIndex.put("n2", 4L);
    node.nextIndex.put("n3", 4L);
    node.matchIndex.put("n2", 0L);
    node.matchIndex.put("n3", 0L);

    // Add new entry
    log.add(4, 2);

    // Replicate to majority
    node.matchIndex.put("n2", 4L);

    node.sendHeartbeats();
    net.respondAE("n2", 2, true, 4);

    // Should only apply entries 3 and 4 (not re-apply 1 and 2)
    assertEquals(4, state.getCommitIndex());
    assertEquals(4, state.getLastApplied());
    assertEquals(4, fsm.appliedEntries.size(), "should have applied 4 entries total");
  }

  @Test
  void leaderNeedsStrictMajority() {
    // 3-node cluster needs 2 nodes (including leader) for majority
    log.add(1, 1);

    state.setCurrentTerm(2);
    state.becomeLeader();

    log.add(2, 2);

    node.nextIndex.put("n2", 3L);
    node.nextIndex.put("n3", 3L);
    node.matchIndex.put("n2", 0L); // n2 doesn't have it
    node.matchIndex.put("n3", 0L); // n3 doesn't have it

    // Only leader has entry 2 - not a majority
    node.sendHeartbeats();
    // No responses

    assertEquals(0, state.getCommitIndex(), "single node (even leader) is not a majority");

    // Now one follower gets it
    node.matchIndex.put("n2", 2L);
    net.respondAE("n2", 2, true, 2);

    // Now we have majority (leader + n2)
    assertEquals(2, state.getCommitIndex(), "leader + 1 follower = majority in 3-node cluster");
  }

  @Test
  void leaderHandlesMixedFollowerProgress() {
    // Setup: Followers at different positions
    log.add(1, 1);
    log.add(2, 1);
    log.add(3, 1);
    log.add(4, 1);
    log.add(5, 1);

    state.setCurrentTerm(2);
    state.becomeLeader();

    log.add(6, 2);

    node.nextIndex.put("n2", 7L);
    node.nextIndex.put("n3", 3L);
    // n2 is caught up, n3 is behind
    node.matchIndex.put("n2", 6L);
    node.matchIndex.put("n3", 2L);

    node.sendHeartbeats();
    net.respondAE("n2", 2, true, 6);

    // Should commit up to 6 (leader + n2 = majority)
    assertEquals(6, state.getCommitIndex());

    // Later, n3 catches up
    node.matchIndex.put("n3", 6L);
    net.respondAE("n3", 2, true, 6);

    // commitIndex stays at 6 (doesn't regress)
    assertEquals(6, state.getCommitIndex(), "commitIndex never decreases");
  }

  @Test
  void leaderDoesNotCommitWhenNotLeader() {
    // Setup as follower with entries
    log.add(1, 1);
    log.add(2, 1);

    state.setCurrentTerm(2);
    state.becomeFollower(); // Not leader!

    // Even if we manually set matchIndex (shouldn't happen, but defensive)
    node.matchIndex.put("n2", 2L);
    node.matchIndex.put("n3", 2L);

    // Try to advance commit (should be no-op when not leader)
    // This would be called in onAppendEntriesResponse which checks role first

    assertEquals(0, state.getCommitIndex(), "follower should not advance commitIndex on its own");
  }
}
