package org.jraft.node;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.jraft.kv.Command;
import org.jraft.kv.KvStateMachine;
import org.jraft.kv.Put;
import org.jraft.net.RaftTransport;
import org.jraft.rpc.AppendEntriesRequest;
import org.jraft.rpc.AppendEntriesResponse;
import org.jraft.rpc.RequestVoteRequest;
import org.jraft.rpc.RequestVoteResponse;
import org.jraft.state.RaftState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.protobuf.ByteString;

/**
 * integration tests for persistence layer wired into raft node.
 *
 * tests the full flow: node starts -> processes requests -> crashes -> recovers
 */
public class PersistenceIntegrationTest {

  @TempDir
  Path tempDir;

  /**
   * test that a node can recover its term and votedFor after restart
   */
  @Test
  void testTermAndVoteRecovery() throws IOException {
    String nodeId = "n1";
    List<String> peers = List.of("n2", "n3");
    Path dataDir = tempDir.resolve("node1");

    // phase 1: create node, increment term, vote for self
    FakeTransport transport1 = new FakeTransport();
    RaftNode node1 = RaftNodeFactory.create(
      nodeId,
      peers,
      dataDir,
      new KvStateMachine(),
      transport1
    );

    // simulate starting an election (increments term, votes for self)
    node1.startElection();
    assertEquals(1, node1.getRaftState().getCurrentTerm());
    assertEquals("n1", node1.getRaftState().getVotedFor());

    // phase 2: simulate crash (no cleanup, just drop reference)
    node1 = null;
    transport1 = null;

    // phase 3: recover from disk
    FakeTransport transport2 = new FakeTransport();
    RaftNode node2 = RaftNodeFactory.create(
      nodeId,
      peers,
      dataDir,
      new KvStateMachine(),
      transport2
    );

    // verify state recovered
    assertEquals(1, node2.getRaftState().getCurrentTerm());
    assertEquals("n1", node2.getRaftState().getVotedFor());
    assertEquals(RaftState.Role.FOLLOWER, node2.getRaftState().getRole()); // always start as follower
  }

  @Test
  void testStateMachineReplayOnRecovery() throws IOException {
    String nodeId = "n1";
    List<String> peers = List.of("n2", "n3");
    Path dataDir = tempDir.resolve("node1");

    FakeTransport transport1 = new FakeTransport();
    KvStateMachine kv1 = new KvStateMachine();
    RaftNode node1 = RaftNodeFactory.create(
      nodeId,
      peers,
      dataDir,
      kv1,
      transport1
    );

    node1.getRaftState().setCurrentTerm(1);
    node1.getRaftState().becomeLeader();
    node1.getRaftState().setLeader(nodeId);
    node1.nextIndex.put("n2", 1L);
    node1.nextIndex.put("n3", 1L);

    byte[] cmd = makeCommand("client-1", 1, "key", "value");
    node1.propose(cmd);

    node1.getRaftState().setCommitIndex(1);
    kv1.apply(node1.getLog().entryAt(1));
    node1.getRaftState().setLastApplied(1);

    assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), kv1.get("key"));

    node1 = null;
    transport1 = null;

    FakeTransport transport2 = new FakeTransport();
    KvStateMachine kv2 = new KvStateMachine();
    RaftNode node2 = RaftNodeFactory.create(
      nodeId,
      peers,
      dataDir,
      kv2,
      transport2
    );

    assertEquals(1, node2.getRaftState().getCommitIndex());
    assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), kv2.get("key"));
  }

  /**
   * test that a node can recover its log entries after restart
   */
  @Test
  void testLogRecovery() throws IOException {
    String nodeId = "n1";
    List<String> peers = List.of("n2", "n3");
    Path dataDir = tempDir.resolve("node1");

    // phase 1: become leader and propose commands
    FakeTransport transport1 = new FakeTransport();
    RaftNode node1 = RaftNodeFactory.create(
      nodeId,
      peers,
      dataDir,
      new KvStateMachine(),
      transport1
    );

    // become leader
    node1.getRaftState().setCurrentTerm(1);
    node1.getRaftState().becomeLeader();
    node1.getRaftState().setLeader(nodeId);
    node1.nextIndex.put("n2", 1L);
    node1.nextIndex.put("n3", 1L);

    // propose some commands
    byte[] cmd1 = makeCommand("client-1", 1, "key1", "value1");
    byte[] cmd2 = makeCommand("client-1", 2, "key2", "value2");
    byte[] cmd3 = makeCommand("client-1", 3, "key3", "value3");

    node1.propose(cmd1);
    node1.propose(cmd2);
    node1.propose(cmd3);

    // verify entries in log
    assertEquals(3, node1.getLog().lastIndex());

    // phase 2: simulate crash
    node1 = null;
    transport1 = null;

    // phase 3: recover
    FakeTransport transport2 = new FakeTransport();
    RaftNode node2 = RaftNodeFactory.create(
      nodeId,
      peers,
      dataDir,
      new KvStateMachine(),
      transport2
    );

    // verify log recovered
    assertEquals(3, node2.getLog().lastIndex());
    assertEquals(1, node2.getLog().termAt(1));
    assertEquals(1, node2.getLog().termAt(2));
    assertEquals(1, node2.getLog().termAt(3));

    // verify can read entries
    assertNotNull(node2.getLog().entryAt(1));
    assertNotNull(node2.getLog().entryAt(2));
    assertNotNull(node2.getLog().entryAt(3));
  }

  /**
   * test that a node can recover after multiple term changes
   */
  @Test
  void testMultipleTermChanges() throws IOException {
    String nodeId = "n1";
    List<String> peers = List.of("n2", "n3");
    Path dataDir = tempDir.resolve("node1");

    // phase 1: go through multiple terms
    FakeTransport transport1 = new FakeTransport();
    RaftNode node1 = RaftNodeFactory.create(
      nodeId,
      peers,
      dataDir,
      new KvStateMachine(),
      transport1
    );

    // term 1: start election
    node1.startElection();
    assertEquals(1, node1.getRaftState().getCurrentTerm());
    assertEquals("n1", node1.getRaftState().getVotedFor());

    // term 2: hear from higher term, step down
    node1.getRaftState().setCurrentTerm(2);
    node1.getRaftState().setVotedFor(null);

    // term 3: start another election
    node1.startElection();
    assertEquals(3, node1.getRaftState().getCurrentTerm());
    assertEquals("n1", node1.getRaftState().getVotedFor());

    // phase 2: crash and recover
    node1 = null;
    transport1 = null;

    FakeTransport transport2 = new FakeTransport();
    RaftNode node2 = RaftNodeFactory.create(
      nodeId,
      peers,
      dataDir,
      new KvStateMachine(),
      transport2
    );

    // should recover final state
    assertEquals(3, node2.getRaftState().getCurrentTerm());
    assertEquals("n1", node2.getRaftState().getVotedFor());
  }

  /**
   * test that a node can continue operating after recovery
   */
  @Test
  void testContinueAfterRecovery() throws IOException {
    String nodeId = "n1";
    List<String> peers = List.of("n2", "n3");
    Path dataDir = tempDir.resolve("node1");

    // phase 1: create node and propose one command
    FakeTransport transport1 = new FakeTransport();
    RaftNode node1 = RaftNodeFactory.create(
      nodeId,
      peers,
      dataDir,
      new KvStateMachine(),
      transport1
    );

    node1.getRaftState().setCurrentTerm(1);
    node1.getRaftState().becomeLeader();
    node1.nextIndex.put("n2", 1L);
    node1.nextIndex.put("n3", 1L);

    byte[] cmd1 = makeCommand("client-1", 1, "key1", "value1");
    node1.propose(cmd1);

    assertEquals(1, node1.getLog().lastIndex());

    // phase 2: crash and recover
    node1 = null;
    transport1 = null;

    FakeTransport transport2 = new FakeTransport();
    RaftNode node2 = RaftNodeFactory.create(
      nodeId,
      peers,
      dataDir,
      new KvStateMachine(),
      transport2
    );

    // phase 3: continue operating - propose more commands
    node2.getRaftState().setCurrentTerm(2);
    node2.getRaftState().becomeLeader();
    node2.nextIndex.put("n2", 2L);
    node2.nextIndex.put("n3", 2L);

    byte[] cmd2 = makeCommand("client-1", 2, "key2", "value2");
    byte[] cmd3 = makeCommand("client-1", 3, "key3", "value3");
    node2.propose(cmd2);
    node2.propose(cmd3);

    // should have all 3 entries
    assertEquals(3, node2.getLog().lastIndex());
    assertEquals(1, node2.getLog().termAt(1)); // old entry
    assertEquals(2, node2.getLog().termAt(2)); // new entry
    assertEquals(2, node2.getLog().termAt(3)); // new entry
  }

  /**
   * test recovery of empty node (fresh start)
   */
  @Test
  void testFreshNodeRecovery() throws IOException {
    String nodeId = "n1";
    List<String> peers = List.of("n2", "n3");
    Path dataDir = tempDir.resolve("node1");

    // create fresh node
    FakeTransport transport = new FakeTransport();
    RaftNode node = RaftNodeFactory.create(
      nodeId,
      peers,
      dataDir,
      new KvStateMachine(),
      transport
    );

    // should start with clean state
    assertEquals(0, node.getRaftState().getCurrentTerm());
    assertEquals(null, node.getRaftState().getVotedFor());
    assertEquals(0, node.getLog().lastIndex());
    assertEquals(RaftState.Role.FOLLOWER, node.getRaftState().getRole());
  }

  // --- helper methods ---

  private byte[] makeCommand(String clientId, long opId, String key, String value) {
    return Command.newBuilder()
      .setClientId(clientId)
      .setOpId(opId)
      .setPut(Put.newBuilder()
        .setKey(key)
        .setValue(ByteString.copyFromUtf8(value)))
      .build()
      .toByteArray();
  }

  /**
   * fake transport that captures sent messages for verification
   */
  private static class FakeTransport implements RaftTransport {
    final Map<String, RequestVoteRequest> voteRequests = new HashMap<>();
    final Map<String, AppendEntriesRequest> appendRequests = new HashMap<>();

    @Override
    public void requestVote(String peerId, RequestVoteRequest req, Consumer<RequestVoteResponse> callback) {
      voteRequests.put(peerId, req);
      // don't call callback - simulating network delay/partition
    }

    @Override
    public void appendEntries(String peerId, AppendEntriesRequest req, Consumer<AppendEntriesResponse> callback) {
      appendRequests.put(peerId, req);
      // don't call callback - simulating network delay/partition
    }
  }
}
