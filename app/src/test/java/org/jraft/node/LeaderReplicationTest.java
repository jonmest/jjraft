package org.jraft.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

import com.google.protobuf.ByteString;

/**
 * Tests for leader log replication to followers.
 *
 * Key behavior tested:
 * - Leader includes actual entries in AppendEntries (not just heartbeats)
 * - Leader sends entries from nextIndex[peer] to lastIndex
 * - Multiple entries batched in single RPC
 * - nextIndex and matchIndex updated correctly on success
 * - Leader backs off nextIndex on failure and retries
 */
public class LeaderReplicationTest {

  private static class FakeTransport implements RaftTransport {
    final Map<String, AppendEntriesRequest> lastAE = new HashMap<>();
    final Map<String, Consumer<AppendEntriesResponse>> aeCb = new HashMap<>();

    @Override
    public void requestVote(String peerId, RequestVoteRequest req, Consumer<RequestVoteResponse> cb) {
      // Not used
    }

    @Override
    public void appendEntries(String peerId, AppendEntriesRequest req, Consumer<AppendEntriesResponse> cb) {
      lastAE.put(peerId, req);
      aeCb.put(peerId, cb);
    }

    void respondAE(String peerId, long term, boolean success) {
      var cb = aeCb.get(peerId);
      if (cb != null) {
        cb.accept(AppendEntriesResponse.newBuilder()
            .setTerm(term)
            .setSuccess(success)
            .build());
      }
    }

    void clear() {
      lastAE.clear();
      aeCb.clear();
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
  void leaderSendsActualEntries_notJustHeartbeats() {
    // Setup: Leader with entries
    log.add(1, 1, ByteString.copyFromUtf8("cmd1"));
    log.add(2, 1, ByteString.copyFromUtf8("cmd2"));
    log.add(3, 1, ByteString.copyFromUtf8("cmd3"));

    state.setCurrentTerm(2);
    state.becomeLeader();

    // Initialize leader state
    node.nextIndex.put("n2", 1L); // n2 needs everything
    node.matchIndex.put("n2", 0L);
    node.nextIndex.put("n3", 1L); // n3 too
    node.matchIndex.put("n3", 0L);

    // Send to n2
    node.sendHeartbeats();

    // Verify: AppendEntries includes actual entries
    var req = net.lastAE.get("n2");
    assertNotNull(req, "should have sent AppendEntries to n2");
    assertEquals(3, req.getEntriesCount(), "should include all 3 entries");
    assertEquals(1, req.getEntries(0).getIndex());
    assertEquals(2, req.getEntries(1).getIndex());
    assertEquals(3, req.getEntries(2).getIndex());
    assertEquals(ByteString.copyFromUtf8("cmd1"), req.getEntries(0).getData());
  }

  @Test
  void leaderSendsOnlyNewEntries() {
    // Setup: Leader with entries
    log.add(1, 1);
    log.add(2, 1);
    log.add(3, 1);
    log.add(4, 2);
    log.add(5, 2);

    state.setCurrentTerm(2);
    state.becomeLeader();

    // n2 already has entries 1-3
    node.nextIndex.put("n2", 4L);
    node.matchIndex.put("n2", 3L);
    node.nextIndex.put("n3", 4L);
    node.matchIndex.put("n3", 3L);

    node.sendHeartbeats();

    var req = net.lastAE.get("n2");
    assertNotNull(req);
    assertEquals(2, req.getEntriesCount(), "should only send entries 4 and 5");
    assertEquals(4, req.getEntries(0).getIndex());
    assertEquals(5, req.getEntries(1).getIndex());
    assertEquals(3, req.getPrevLogIndex(), "prevIndex should be 3");
    assertEquals(1, req.getPrevLogTerm(), "prevTerm should be term of entry 3");
  }

  @Test
  void leaderSendsEmptyHeartbeatWhenCaughtUp() {
    // Setup: Leader with entries
    log.add(1, 1);
    log.add(2, 1);
    log.add(3, 2);

    state.setCurrentTerm(2);
    state.becomeLeader();

    // n2 is fully caught up
    node.nextIndex.put("n2", 4L); // next would be 4, but we only have up to 3
    node.matchIndex.put("n2", 3L);
    node.nextIndex.put("n3", 4L);
    node.matchIndex.put("n3", 3L);

    node.sendHeartbeats();

    var req = net.lastAE.get("n2");
    assertNotNull(req);
    assertEquals(0, req.getEntriesCount(), "should send empty heartbeat when caught up");
    assertEquals(3, req.getPrevLogIndex());
    assertEquals(2, req.getPrevLogTerm());
  }

  @Test
  void leaderUpdatesMatchIndexOnSuccess() {
    // Setup: Leader with entries
    log.add(1, 1);
    log.add(2, 1);
    log.add(3, 1);

    state.setCurrentTerm(2);
    state.becomeLeader();

    node.nextIndex.put("n2", 1L);
    node.matchIndex.put("n2", 0L);
    node.nextIndex.put("n3", 1L);
    node.matchIndex.put("n3", 0L);

    node.sendHeartbeats();

    // n2 successfully appends all 3 entries
    net.respondAE("n2", 2, true);

    // Verify: matchIndex and nextIndex updated
    assertEquals(3L, node.matchIndex.get("n2"), "matchIndex should be updated to 3");
    assertEquals(4L, node.nextIndex.get("n2"), "nextIndex should be 4 (last + 1)");
  }

  @Test
  void leaderBacksOffOnFailureAndRetries() {
    // Setup: Leader with entries
    log.add(1, 1);
    log.add(2, 1);
    log.add(3, 2);

    state.setCurrentTerm(2);
    state.becomeLeader();

    node.nextIndex.put("n2", 3L);
    node.matchIndex.put("n2", 0L);
    node.nextIndex.put("n3", 3L);
    node.matchIndex.put("n3", 0L);

    // First attempt
    node.sendHeartbeats();
    var req1 = net.lastAE.get("n2");
    assertEquals(2, req1.getPrevLogIndex(), "first attempt uses prevIndex = nextIndex - 1");

    // Failure response (this triggers backoff and immediate retry)
    net.respondAE("n2", 2, false);

    // Verify: nextIndex backed off and retry sent
    assertEquals(2L, node.nextIndex.get("n2"), "nextIndex should decrement on failure");

    var req2 = net.lastAE.get("n2");
    assertNotNull(req2, "should automatically retry after backoff");
    assertEquals(1, req2.getPrevLogIndex(), "retry uses new prevIndex");
  }

  @Test
  void leaderSendsToAllPeers() {
    log.add(1, 1);

    state.setCurrentTerm(2);
    state.becomeLeader();

    node.nextIndex.put("n2", 1L);
    node.nextIndex.put("n3", 1L);
    node.matchIndex.put("n2", 0L);
    node.matchIndex.put("n3", 0L);

    node.sendHeartbeats();

    // Verify: sent to both peers
    assertNotNull(net.lastAE.get("n2"), "should send to n2");
    assertNotNull(net.lastAE.get("n3"), "should send to n3");
  }

  @Test
  void leaderIncludesCommitIndexInAppendEntries() {
    log.add(1, 1);
    log.add(2, 1);

    state.setCurrentTerm(2);
    state.becomeLeader();
    state.setCommitIndex(1); // Leader has committed up to 1

    node.nextIndex.put("n2", 1L);
    node.matchIndex.put("n2", 0L);
    node.nextIndex.put("n3", 1L);
    node.matchIndex.put("n3", 0L);

    node.sendHeartbeats();

    var req = net.lastAE.get("n2");
    assertEquals(1, req.getLeaderCommit(), "should include leader's commitIndex");
  }

  @Test
  void leaderStopsBackoffAtIndex1() {
    log.add(1, 1);
    log.add(2, 1);

    state.setCurrentTerm(2);
    state.becomeLeader();

    // Start with nextIndex = 2
    node.nextIndex.put("n2", 2L);
    node.matchIndex.put("n2", 0L);
    node.nextIndex.put("n3", 2L);
    node.matchIndex.put("n3", 0L);

    // First failure: backs off to 1
    node.sendHeartbeats();
    net.respondAE("n2", 2, false);

    assertEquals(1L, node.nextIndex.get("n2"));

    // Backoff triggered automatic retry, respond to that
    net.respondAE("n2", 2, false);

    assertEquals(1L, node.nextIndex.get("n2"), "nextIndex should not go below 1");

    // Verify retry still sent even at minimum
    assertNotNull(net.lastAE.get("n2"), "should retry even when nextIndex is 1");
  }

  @Test
  void leaderSendsDifferentEntriesToDifferentPeers() {
    log.add(1, 1);
    log.add(2, 1);
    log.add(3, 1);
    log.add(4, 2);

    state.setCurrentTerm(2);
    state.becomeLeader();

    // n2 is behind, needs entries 2-4
    node.nextIndex.put("n2", 2L);
    node.matchIndex.put("n2", 1L);

    // n3 is caught up, needs only entry 4
    node.nextIndex.put("n3", 4L);
    node.matchIndex.put("n3", 3L);

    node.sendHeartbeats();

    // Verify different entries sent
    var req2 = net.lastAE.get("n2");
    assertEquals(3, req2.getEntriesCount(), "n2 should get 3 entries");
    assertEquals(2, req2.getEntries(0).getIndex());

    var req3 = net.lastAE.get("n3");
    assertEquals(1, req3.getEntriesCount(), "n3 should get 1 entry");
    assertEquals(4, req3.getEntries(0).getIndex());
  }

  @Test
  void leaderIncludesCurrentTermInEntries() {
    state.setCurrentTerm(5);
    state.becomeLeader();

    // Manually add entry with current term
    log.add(1, 5);

    node.nextIndex.put("n2", 1L);
    node.matchIndex.put("n2", 0L);
    node.nextIndex.put("n3", 1L);
    node.matchIndex.put("n3", 0L);

    node.sendHeartbeats();

    var req = net.lastAE.get("n2");
    assertEquals(5, req.getTerm(), "AppendEntries term should match current term");
    assertEquals(5, req.getEntries(0).getTerm(), "entry term should match current term");
  }
}
