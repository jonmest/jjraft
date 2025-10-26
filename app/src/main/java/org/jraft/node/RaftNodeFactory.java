package org.jraft.node;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import org.jraft.core.RepeatingTask;
import org.jraft.core.StateMachine;
import org.jraft.net.RaftTransport;
import org.jraft.state.FileLogStore;
import org.jraft.state.LogStore;
import org.jraft.state.PersistentState;
import org.jraft.state.RaftState;
import org.jraft.util.ElectionTimer;
import org.jraft.util.ExecutorElectionTimer;
import org.jraft.util.ExecutorRepeatingTask;

/**
 * factory for creating raft nodes with proper persistence wiring.
 *
 * this handles:
 *   - loading persistent state (term, votedFor) from disk
 *   - recovering log from disk or creating fresh storage
 *   - wiring persistence hooks into RaftState
 *   - creating timer implementations
 *
 * usage:
 *   RaftNode node = RaftNodeFactory.create(
 *     "node-1",
 *     List.of("node-2", "node-3"),
 *     Paths.get("/data/node-1"),
 *     new KvStateMachine(),
 *     new GrpcTransport()
 *   );
 */
public class RaftNodeFactory {

  /**
   * create a raft node with persistent storage at the given data directory
   */
  public static RaftNode create(
      String nodeId,
      List<String> peers,
      Path dataDir,
      StateMachine stateMachine,
      RaftTransport transport) throws IOException {

    Files.createDirectories(dataDir);

    // load or initialize persistent state (term, votedFor)
    PersistentState persistentState = PersistentState.load(dataDir);

    // recover or create log storage
    Path logDir = dataDir.resolve("log");
    LogStore log = new FileLogStore(logDir);

    // create raft state and wire persistence hooks
    // initialize directly to avoid triggering persistence on load
    // commitIndex starts at 0 - it will be set by leader via AppendEntries
    PersistentRaftState raftState = new PersistentRaftState(
      persistentState,
      persistentState.getCurrentTerm(),
      persistentState.getVotedFor()
    );

    // note: commitIndex is volatile and starts at 0 on recovery
    // however, we conservatively replay all log entries to the state machine
    // this provides better recovery UX and is safe if operations are idempotent
    if (stateMachine != null && log.lastIndex() > 0) {
      for (long i = 1; i <= log.lastIndex(); i++) {
        var entry = log.entryAt(i);
        if (entry != null) {
          stateMachine.apply(entry);
          raftState.setCommitIndex(i);
          raftState.setLastApplied(i);
        }
      }
    }

    // create timer implementations
    ElectionTimer electionTimer = new ExecutorElectionTimer("raft-election-" + nodeId);
    RepeatingTask heartbeatTask = new ExecutorRepeatingTask();

    return new RaftNode(
      nodeId,
      peers,
      raftState,
      log,
      transport,
      stateMachine,
      heartbeatTask,
      electionTimer
    );
  }

  /**
   * raft state that automatically persists term and votedFor changes
   */
  private static class PersistentRaftState extends RaftState {
    private final PersistentState persistentState;

    public PersistentRaftState(PersistentState persistentState, long initialTerm, String initialVotedFor) {
      this.persistentState = persistentState;
      // initialize fields directly without triggering persistence
      this.currentTerm = initialTerm;
      this.votedFor = initialVotedFor;
      // commitIndex starts at 0 (volatile state, not persisted)
    }

    @Override
    public void setCurrentTerm(long term) {
      if (this.currentTerm == term) return;

      super.setCurrentTerm(term);
      try {
        persistentState.save(getCurrentTerm(), getVotedFor());
      } catch (IOException e) {
        throw new RuntimeException("failed to persist term change", e);
      }
    }

    @Override
    public void setVotedFor(String votedFor) {
      if (Objects.equals(this.votedFor, votedFor)) return;

      super.setVotedFor(votedFor);
      try {
        persistentState.save(getCurrentTerm(), getVotedFor());
      } catch (IOException e) {
        throw new RuntimeException("failed to persist vote change", e);
      }
    }

    // commitIndex is volatile state - not persisted
    // it will be reconstructed from leader's AppendEntries messages

    @Override
    public void clearVote() {
      if (this.votedFor == null) return;

      super.clearVote();
      try {
        persistentState.save(getCurrentTerm(), getVotedFor());
      } catch (IOException e) {
        throw new RuntimeException("failed to persist vote clear", e);
      }
    }
  }
}
