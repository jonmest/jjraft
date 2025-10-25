package org.jraft.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.jraft.core.util.FakeRepeatingTask;
import org.jraft.net.RaftTransport;
import org.jraft.node.RaftNode;
import org.jraft.rpc.AppendEntriesRequest;
import org.jraft.rpc.AppendEntriesResponse;
import org.jraft.rpc.LogEntry;
import org.jraft.rpc.NodeId;
import org.jraft.rpc.RequestVoteRequest;
import org.jraft.rpc.RequestVoteResponse;
import org.jraft.state.LogStore;
import org.jraft.state.RaftState;
import org.jraft.util.FakeElectionTimer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives RaftNode’s election flow with a fake transport.
 * Assumes RaftNode has:
 *   - onElectionTick()
 *   - sendHeartbeats()
 *   - constructor: (id, peers, state, log, transport)
 * RaftState is a simple POJO with getters/setters and Role.
 */
final class ElectionFlowTest {

  private static class FollowerLog {
    // 1-based index → term (0 for “no entry”)
    final Map<Long,Long> termsByIndex = new HashMap<>();
    long termAt(long idx) { return termsByIndex.getOrDefault(idx, 0L); }
  }

  private void driveDivergentFollowerToConvergence(String peer, FollowerLog flog) {
    while (true) {
      AppendEntriesRequest req = NET.lastAE.get(peer);
      assertNotNull(req, "leader must have sent AE to " + peer);
      long prevIdx = req.getPrevLogIndex();
      long prevTerm = req.getPrevLogTerm();
      boolean ok = (prevIdx == 0) || (flog.termAt(prevIdx) == prevTerm);
      NET.respondAE(peer, S.getCurrentTerm(), ok);
      if (ok) break;  // leader should stop backing off after match
      // on failure, leader will immediately send a new AE
      // loop again to read the new lastAE and respond accordingly
    }
  }

  /** In-memory log for tests: 1-based indices, termAt(0)=0, lastIndex()=size. */
  static final class MemLog implements LogStore {
    final ArrayList<LogEntry> a = new ArrayList<>();
    @Override public long lastIndex() { return a.size(); }
    @Override public long termAt(long index) {
      if (index <= 0 || index > a.size()) return 0;
      return a.get((int) index - 1).getTerm();
    }
    @Override public void append(List<LogEntry> batch) {
      if (batch.isEmpty()) return;
      long expected = lastIndex() + 1;
      if (batch.get(0).getIndex() != expected) {
        throw new IllegalStateException("append not contiguous: got " + batch.get(0).getIndex() + " expected " + expected);
      }
      a.addAll(batch);
    }
    @Override public void truncateFrom(long index) {
      if (index <= 0) { a.clear(); return; }
      while (a.size() >= index) a.remove(a.size() - 1);
    }
    @Override public LogEntry entryAt(long index) {
      if (index <= 0 || index > a.size()) return null;
      return a.get((int) index - 1);
    }
    // helper
    void add(long idx, long term) {
      a.add(LogEntry.newBuilder().setIndex(idx).setTerm(term).build());
    }
  }

  /** Simple mutable state for tests. Adjust to your RaftState API as needed. */
  static final class TdState extends RaftState {
    private long currentTerm, commitIndex, lastApplied;
    private String votedFor, leaderId;
    private Role role = Role.FOLLOWER;

    public long getCurrentTerm() { return currentTerm; }
    public void setCurrentTerm(long t) { currentTerm = t; }
    public String getVotedFor() { return votedFor; }
    public void setVotedFor(String v) { votedFor = v; }
    public void clearVote() { votedFor = null; }

    public Role getRole() { return role; }
    public void setRole(Role r) { role = r; }

    @Override public void becomeFollower() { role = Role.FOLLOWER; leaderId = null; }
    @Override public void becomeCandidate() { role = Role.CANDIDATE; leaderId = null; }   // <- add this
    @Override public void becomeLeader() { role = Role.LEADER; }                           // <- and this

    public long getCommitIndex() { return commitIndex; }
    public void setCommitIndex(long i) { commitIndex = i; }
    public long getLastApplied() { return lastApplied; }
    public void setLastApplied(long i) { lastApplied = i; }
    public String getLeader() { return leaderId; }
    public void setLeader(String id) { leaderId = id; }
  }


  /** Fake transport captures requests and lets tests trigger callbacks. */
  static final class FakeTransport implements RaftTransport {
    final Map<String, RequestVoteRequest> lastRV = new HashMap<>();
    final Map<String, Consumer<RequestVoteResponse>> rvCb = new HashMap<>();
    final Map<String, AppendEntriesRequest> lastAE = new HashMap<>();
    final Map<String, Consumer<AppendEntriesResponse>> aeCb = new HashMap<>();
    public final Map<String,Integer> aeSends = new HashMap<>();

    @Override
    public void requestVote(String peerId, RequestVoteRequest req, Consumer<RequestVoteResponse> cb) {
      lastRV.put(peerId, req);
      rvCb.put(peerId, cb);
    }

    @Override
    public void appendEntries(String peerId, AppendEntriesRequest req, Consumer<AppendEntriesResponse> cb) {
      lastAE.put(peerId, req);
      aeCb.put(peerId, cb);
      aeSends.put(peerId, aeSends.getOrDefault(peerId, 0) + 1);   // NEW
    }

    void respondVote(String peerId, long term, boolean grant) {
      var cb = rvCb.get(peerId);
      assertNotNull(cb, "no RV callback for " + peerId);
      cb.accept(RequestVoteResponse.newBuilder().setTerm(term).setVoteGranted(grant).build());
    }

    void respondAE(String peerId, long term, boolean success) {
      var cb = aeCb.get(peerId);
      assertNotNull(cb, "no AE callback for " + peerId);
      cb.accept(AppendEntriesResponse.newBuilder().setTerm(term).setSuccess(success).build());
    }
  }

  // --- Shared fixtures -------------------------------------------------------

  private TdState S;
  private MemLog LOG;
  private FakeTransport NET;
  private FakeElectionTimer ET;
  private FakeRepeatingTask HB;
  private RaftNode node;
  private final String self = "n1";
  private final List<String> peers = List.of("n2", "n3");

  @BeforeEach
  void setup() {
    S = new TdState();
    LOG = new MemLog();
    NET = new FakeTransport();
    ET = new FakeElectionTimer();
    HB = new FakeRepeatingTask();                
    node = new RaftNode(self, peers, S, LOG, NET, null, HB, ET);
  }

  // --- Tests -----------------------------------------------------------------

  @Test
  void becomesLeaderOnMajority() {
    node.onElectionTick();
    long electionTerm = S.getCurrentTerm();
    NET.respondVote("n2", electionTerm, true);
    NET.respondVote("n3", electionTerm, true);

    assertEquals(RaftState.Role.LEADER, S.getRole(), "should become leader");

    assertTrue(NET.lastAE.containsKey("n2"));
    int afterImmediate = NET.lastAE.size();

    HB.tickOnce();
    AppendEntriesRequest ae2 = NET.lastAE.get("n2");
    assertEquals(electionTerm, ae2.getTerm());

    HB.tickOnce();
  }

  @Test
  void schedulerStopsOnStepDown() {
    node.onElectionTick();
    long term = S.getCurrentTerm();
    NET.respondVote("n2", term, true);
    NET.respondVote("n3", term, true);
    assertEquals(RaftState.Role.LEADER, S.getRole());
    assertTrue(HB.isRunning(), "heartbeat task should be running after election");

    int before = NET.aeSends.getOrDefault("n2", 0);
    HB.tickOnce();
    assertEquals(before + 1, NET.aeSends.get("n2"), "tick while leader must send AE");

    node.sendHeartbeats();
    NET.respondAE("n2", term + 1, false);

    assertEquals(RaftState.Role.FOLLOWER, S.getRole());
    assertFalse(HB.isRunning(), "heartbeat task must stop on stepDown");

    int after = NET.aeSends.getOrDefault("n2", 0);
    HB.tickOnce();
    assertEquals(after, NET.aeSends.get("n2"), "tick after stop must not send AE");
  }


  @Test
  void stepsDownOnHigherTermInVoteResponse() {
    node.onElectionTick();
    long t = S.getCurrentTerm();

    NET.respondVote("n2", t + 1, false);

    assertEquals(RaftState.Role.FOLLOWER, S.getRole(), "must step down on higher-term RV");
    assertEquals(t + 1, S.getCurrentTerm(), "term should advance to the higher term");
    assertNull(S.getVotedFor(), "vote reset on step down");
  }

  @Test
  void stepsDownOnHigherTermInAppendEntriesResponse() {
    node.onElectionTick();
    long term = S.getCurrentTerm();
    NET.respondVote("n2", term, true);
    NET.respondVote("n3", term, true);
    assertEquals(RaftState.Role.LEADER, S.getRole());

    node.sendHeartbeats();

    NET.respondAE("n2", term + 1, false);

    assertEquals(RaftState.Role.FOLLOWER, S.getRole(), "higher-term AE response forces step down");
    assertEquals(term + 1, S.getCurrentTerm());
  }

  @Test
  void electionUsesLastLogPositionInRequest() {
    LOG.add(1, 3);
    LOG.add(2, 3);
    LOG.add(3, 4);

    node.onElectionTick();

    var rv2 = NET.lastRV.get("n2");
    assertNotNull(rv2);
    assertEquals(LOG.lastIndex(), rv2.getLastLogIndex());
    assertEquals(LOG.termAt(LOG.lastIndex()), rv2.getLastLogTerm());
    assertEquals(S.getCurrentTerm(), rv2.getTerm());
    assertEquals(self, rv2.getCandidateId().getId());
  }

  @Test
  void electionTimerResetsOnAppendEntriesWithTermGte() {
    var req = AppendEntriesRequest.newBuilder()
        .setTerm(1)
        .setLeaderId(NodeId.newBuilder().setId("n2"))
        .setPrevLogIndex(5)
        .setPrevLogTerm(999)
        .setLeaderCommit(0)
        .build();

    int before = node.getElectionResetCount();
    var resp = node.onAppendEntriesRequest(req);

    assertFalse(resp.getSuccess(), "prev mismatch should fail");
    assertEquals(1, S.getCurrentTerm(), "term should update to 1");
    assertEquals(before + 1, node.getElectionResetCount(),
        "reset must happen even on prev-link mismatch when term >= current");
  }

  @Test
  void electionTimerDoesNotResetOnStaleAppendEntries() {
    S.setCurrentTerm(3);
    var stale = AppendEntriesRequest.newBuilder()
        .setTerm(2) // lower
        .setLeaderId(NodeId.newBuilder().setId("nX"))
        .build();

    int before = node.getElectionResetCount();
    var resp = node.onAppendEntriesRequest(stale);

    assertFalse(resp.getSuccess());
    assertEquals(3, S.getCurrentTerm());
    assertEquals(before, node.getElectionResetCount(), "no reset on stale AE");
  }

  @Test
  void electionTimerResetsOnlyWhenVoteGranted() {
    S.setCurrentTerm(4);
    LOG.a.clear();

    var goodRV = RequestVoteRequest.newBuilder()
        .setTerm(4)
        .setCandidateId(NodeId.newBuilder().setId("c1"))
        .setLastLogIndex(0)
        .setLastLogTerm(0)
        .build();

    int before = node.getElectionResetCount();
    var resp1 = node.onRequestVoteRequest(goodRV);
    assertTrue(resp1.getVoteGranted());
    assertEquals(before + 1, node.getElectionResetCount(), "reset when vote granted");

    var deniedRV = RequestVoteRequest.newBuilder()
        .setTerm(4)
        .setCandidateId(NodeId.newBuilder().setId("c2"))
        .setLastLogIndex(999)
        .setLastLogTerm(999)
        .build();

    int before2 = node.getElectionResetCount();
    var resp2 = node.onRequestVoteRequest(deniedRV);
    assertFalse(resp2.getVoteGranted());
    assertEquals(before2, node.getElectionResetCount(), "no reset when vote denied");

    var staleRV = RequestVoteRequest.newBuilder()
        .setTerm(3)
        .setCandidateId(NodeId.newBuilder().setId("c3"))
        .setLastLogIndex(0)
        .setLastLogTerm(0)
        .build();

    int before3 = node.getElectionResetCount();
    var resp3 = node.onRequestVoteRequest(staleRV);
    assertFalse(resp3.getVoteGranted());
    assertEquals(before3, node.getElectionResetCount(), "no reset on stale RV");
  }

  @Test
  void leaderBacksOffUntilPrevLinkMatches_thenSucceeds() {
    // 1) Prepare leader log BEFORE election
    LOG.a.clear();
    LOG.add(1, 1);
    LOG.add(2, 1);
    LOG.add(3, 2);
    LOG.add(4, 3);
    LOG.add(5, 4); // lastIndex = 5

    // 2) Elect leader
    node.onElectionTick();
    long t = S.getCurrentTerm();
    NET.respondVote("n2", t, true);
    NET.respondVote("n3", t, true);
    assertEquals(RaftState.Role.LEADER, S.getRole());

    // 3) Make follower n2 divergent/ahead (prevIdx=5,4 mismatching; idx=3 matches)
    FollowerLog flog = new FollowerLog();
    flog.termsByIndex.put(1L, 1L);
    flog.termsByIndex.put(2L, 1L);
    flog.termsByIndex.put(3L, 2L);
    flog.termsByIndex.put(4L, 9L);
    flog.termsByIndex.put(5L, 9L);
    flog.termsByIndex.put(6L, 9L);

    // 4) Trigger a heartbeat, then drive responses until the leader finds a match
    node.sendHeartbeats();
    driveDivergentFollowerToConvergence("n2", flog);

    // 5) After success at prevIdx=3, leader sends entries [4,5]:
    // matchIndex[n2] == 5 (prevIdx 3 + 2 entries), nextIndex[n2] == 6
    assertEquals(6L, node.nextIndex.get("n2"));
    assertEquals(5L, node.matchIndex.get("n2"));
  }


}
