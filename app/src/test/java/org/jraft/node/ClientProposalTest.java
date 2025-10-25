package org.jraft.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.jraft.core.util.FakeRepeatingTask;
import org.jraft.kv.Command;
import org.jraft.kv.Put;
import org.jraft.net.RaftTransport;
import org.jraft.rpc.AppendEntriesRequest;
import org.jraft.rpc.AppendEntriesResponse;
import org.jraft.rpc.LogEntry;
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
 * Tests for client proposal interface.
 *
 * Key behavior tested:
 * - propose() rejects when not leader (returns -1)
 * - propose() appends entry to log with correct index and term
 * - propose() triggers immediate replication
 * - End-to-end: proposal -> replication -> commit -> FSM application
 */
public class ClientProposalTest {

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
  void proposeRejectsWhenFollower() {
    // Start as follower
    state.becomeFollower();
    state.setCurrentTerm(1);

    byte[] data = "some command".getBytes();
    long index = node.propose(data);

    assertEquals(-1, index, "propose should return -1 when not leader");
    assertEquals(0, log.lastIndex(), "log should not be modified");
  }

  @Test
  void proposeRejectsWhenCandidate() {
    // Start as candidate
    state.becomeCandidate();
    state.setCurrentTerm(2);

    byte[] data = "some command".getBytes();
    long index = node.propose(data);

    assertEquals(-1, index, "propose should return -1 when candidate");
    assertEquals(0, log.lastIndex(), "log should not be modified");
  }

  @Test
  void proposeAppendsToLogWhenLeader() {
    // Become leader
    state.setCurrentTerm(3);
    state.becomeLeader();

    node.nextIndex.put("n2", 1L);
    node.nextIndex.put("n3", 1L);

    byte[] data = ByteString.copyFromUtf8("PUT key=value").toByteArray();
    long index = node.propose(data);

    // Verify: returns correct index
    assertEquals(1, index, "should return index where entry was appended");

    // Verify: entry in log
    assertEquals(1, log.lastIndex(), "log should have 1 entry");
    LogEntry entry = log.entryAt(1);
    assertNotNull(entry);
    assertEquals(1, entry.getIndex());
    assertEquals(3, entry.getTerm(), "entry should have current term");
    assertEquals(ByteString.copyFrom(data), entry.getData());
  }

  @Test
  void proposeAppendsMultipleEntries() {
    state.setCurrentTerm(2);
    state.becomeLeader();

    node.nextIndex.put("n2", 1L);
    node.nextIndex.put("n3", 1L);

    // Propose multiple commands
    long idx1 = node.propose("cmd1".getBytes());
    long idx2 = node.propose("cmd2".getBytes());
    long idx3 = node.propose("cmd3".getBytes());

    assertEquals(1, idx1);
    assertEquals(2, idx2);
    assertEquals(3, idx3);
    assertEquals(3, log.lastIndex(), "log should have 3 entries");
  }

  @Test
  void proposeTriggersReplication() {
    state.setCurrentTerm(4);
    state.becomeLeader();

    node.nextIndex.put("n2", 1L);
    node.nextIndex.put("n3", 1L);

    byte[] data = "test command".getBytes();
    node.propose(data);

    // Verify: AppendEntries sent to followers
    AppendEntriesRequest req2 = net.lastAE.get("n2");
    assertNotNull(req2, "should send AppendEntries to n2");
    assertEquals(1, req2.getEntriesCount(), "should include the new entry");
    assertEquals(1, req2.getEntries(0).getIndex());

    AppendEntriesRequest req3 = net.lastAE.get("n3");
    assertNotNull(req3, "should send AppendEntries to n3");
    assertEquals(1, req3.getEntriesCount());
  }

  @Test
  void endToEndProposalFlow() {
    // Setup: Become leader
    state.setCurrentTerm(5);
    state.becomeLeader();

    node.nextIndex.put("n2", 1L);
    node.nextIndex.put("n3", 1L);
    node.matchIndex.put("n2", 0L);
    node.matchIndex.put("n3", 0L);

    // Step 1: Client proposes
    byte[] cmdData = Command.newBuilder()
        .setClientId("client-1")
        .setOpId(1)
        .setPut(Put.newBuilder()
            .setKey("foo")
            .setValue(ByteString.copyFromUtf8("bar")))
        .build()
        .toByteArray();

    long index = node.propose(cmdData);
    assertEquals(1, index);

    // Step 2: Replication happens (automatic via propose)
    // Verify entries were sent
    assertNotNull(net.lastAE.get("n2"));
    assertNotNull(net.lastAE.get("n3"));

    // Step 3: Majority responds successfully
    net.respondAE("n2", 5, true);

    // Step 4: Leader commits and applies
    assertEquals(1, state.getCommitIndex(), "should commit after majority replicates");
    assertEquals(1, state.getLastApplied(), "should apply committed entry");
    assertEquals(1, fsm.appliedEntries.size(), "FSM should receive the command");

    // Verify the command data made it through
    LogEntry appliedEntry = fsm.appliedEntries.get(0);
    assertEquals(ByteString.copyFrom(cmdData), appliedEntry.getData());
  }

  @Test
  void proposeAppendsToExistingLog() {
    // Setup: Log already has entries
    log.add(1, 1);
    log.add(2, 1);
    log.add(3, 2);

    state.setCurrentTerm(3);
    state.becomeLeader();

    node.nextIndex.put("n2", 4L);
    node.nextIndex.put("n3", 4L);

    // Propose new entry
    long index = node.propose("new command".getBytes());

    assertEquals(4, index, "should append after existing entries");
    assertEquals(4, log.lastIndex());
    assertEquals(3, log.entryAt(4).getTerm(), "new entry should have current term");
  }

  @Test
  void proposeWithZeroLengthData() {
    state.setCurrentTerm(1);
    state.becomeLeader();

    node.nextIndex.put("n2", 1L);
    node.nextIndex.put("n3", 1L);

    byte[] emptyData = new byte[0];
    long index = node.propose(emptyData);

    assertEquals(1, index);
    assertEquals(1, log.lastIndex());
    assertEquals(ByteString.EMPTY, log.entryAt(1).getData());
  }

  @Test
  void multipleConcurrentProposals() {
    state.setCurrentTerm(2);
    state.becomeLeader();

    node.nextIndex.put("n2", 1L);
    node.nextIndex.put("n3", 1L);

    // Simulate multiple clients proposing concurrently
    long idx1 = node.propose("client1-cmd1".getBytes());
    long idx2 = node.propose("client2-cmd1".getBytes());
    long idx3 = node.propose("client1-cmd2".getBytes());
    long idx4 = node.propose("client3-cmd1".getBytes());

    // All should get unique sequential indices
    assertEquals(1, idx1);
    assertEquals(2, idx2);
    assertEquals(3, idx3);
    assertEquals(4, idx4);
    assertEquals(4, log.lastIndex());

    // All entries should have current term
    for (int i = 1; i <= 4; i++) {
      assertEquals(2, log.entryAt(i).getTerm());
    }
  }

  @Test
  void proposeAfterSteppingDown() {
    // Start as leader
    state.setCurrentTerm(3);
    state.becomeLeader();

    node.nextIndex.put("n2", 1L);
    node.nextIndex.put("n3", 1L);

    // First proposal succeeds
    long idx1 = node.propose("cmd1".getBytes());
    assertEquals(1, idx1);

    // Step down to follower (e.g., discovered higher term)
    state.setCurrentTerm(4);
    state.becomeFollower();

    // Next proposal should fail
    long idx2 = node.propose("cmd2".getBytes());
    assertEquals(-1, idx2, "propose should fail after stepping down");
    assertEquals(1, log.lastIndex(), "log should not grow");
  }

  @Test
  void proposedEntryIncludedInSubsequentHeartbeats() {
    state.setCurrentTerm(2);
    state.becomeLeader();

    node.nextIndex.put("n2", 1L);
    node.nextIndex.put("n3", 1L);

    // Propose
    node.propose("test".getBytes());

    // Clear previous AppendEntries
    net.lastAE.clear();

    // Subsequent heartbeat should include the entry
    node.sendHeartbeats();

    AppendEntriesRequest req = net.lastAE.get("n2");
    assertNotNull(req);
    assertTrue(req.getEntriesCount() > 0, "heartbeat should include unacknowledged entries");
  }
}
