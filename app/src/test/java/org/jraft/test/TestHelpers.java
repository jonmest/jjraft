package org.jraft.test;

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import org.jraft.core.StateMachine;
import org.jraft.rpc.LogEntry;
import org.jraft.state.LogStore;
import org.jraft.state.RaftState;

/**
 * Shared test utilities and helpers for Raft unit tests.
 */
public final class TestHelpers {
  private TestHelpers() {}

  /**
   * In-memory log implementation for testing.
   * Uses 1-based indexing: index 0 is "empty", termAt(0) = 0.
   */
  public static final class MemLog implements LogStore {
    public final ArrayList<LogEntry> entries = new ArrayList<>();

    @Override
    public long lastIndex() {
      return entries.size();
    }

    @Override
    public long termAt(long index) {
      if (index <= 0 || index > entries.size()) return 0;
      return entries.get((int) index - 1).getTerm();
    }

    @Override
    public void append(List<LogEntry> batch) {
      if (batch.isEmpty()) return;
      long expected = lastIndex() + 1;
      if (batch.get(0).getIndex() != expected) {
        throw new IllegalStateException(
          "append not contiguous: got " + batch.get(0).getIndex() + " expected " + expected);
      }
      entries.addAll(batch);
    }

    @Override
    public void truncateFrom(long index) {
      if (index <= 0) {
        entries.clear();
        return;
      }
      while (entries.size() >= index) {
        entries.remove(entries.size() - 1);
      }
    }

    @Override
    public LogEntry entryAt(long index) {
      if (index <= 0 || index > entries.size()) return null;
      return entries.get((int) index - 1);
    }

    /**
     * Helper to add an entry to the log for test setup.
     */
    public void add(long index, long term) {
      add(index, term, ByteString.EMPTY);
    }

    /**
     * Helper to add an entry with data to the log for test setup.
     */
    public void add(long index, long term, ByteString data) {
      entries.add(LogEntry.newBuilder()
          .setIndex(index)
          .setTerm(term)
          .setData(data)
          .build());
    }

    /**
     * Clear all entries from the log.
     */
    public void clear() {
      entries.clear();
    }
  }

  /**
   * Minimal RaftState implementation for testing.
   * Mutable fields with getters/setters matching the RaftState interface.
   */
  public static class TestRaftState extends RaftState {
    private long currentTerm = 0;
    private long commitIndex = 0;
    private long lastApplied = 0;
    private String votedFor = null;
    private String leaderId = null;
    private Role role = Role.FOLLOWER;

    @Override public long getCurrentTerm() { return currentTerm; }
    @Override public void setCurrentTerm(long t) { currentTerm = t; }

    @Override public String getVotedFor() { return votedFor; }
    @Override public void setVotedFor(String id) { votedFor = id; }
    @Override public void clearVote() { votedFor = null; }

    @Override public Role getRole() { return role; }
    @Override public void setRole(Role r) { role = r; }

    @Override public void becomeFollower() {
      role = Role.FOLLOWER;
      leaderId = null;
    }

    @Override public void becomeCandidate() {
      role = Role.CANDIDATE;
      leaderId = null;
    }

    @Override public void becomeLeader() {
      role = Role.LEADER;
    }

    @Override public long getCommitIndex() { return commitIndex; }
    @Override public void setCommitIndex(long i) { commitIndex = i; }

    @Override public long getLastApplied() { return lastApplied; }
    @Override public void setLastApplied(long i) { lastApplied = i; }

    @Override public String getLeader() { return leaderId; }
    @Override public void setLeader(String id) { leaderId = id; }
  }

  /**
   * No-op state machine for tests that don't care about FSM logic.
   */
  public static final class NoOpStateMachine implements StateMachine {
    @Override
    public ApplyResult apply(LogEntry e) {
      return new ApplyResult(e.getIndex(), true, new byte[]{}, false);
    }
  }

  /**
   * Tracking state machine that records all applied entries for verification.
   */
  public static final class TrackingStateMachine implements StateMachine {
    public final List<LogEntry> appliedEntries = new ArrayList<>();

    @Override
    public ApplyResult apply(LogEntry e) {
      appliedEntries.add(e);
      return new ApplyResult(e.getIndex(), true, new byte[]{}, false);
    }

    public void clear() {
      appliedEntries.clear();
    }
  }

  /**
   * Create a log entry for testing.
   */
  public static LogEntry makeEntry(long index, long term) {
    return makeEntry(index, term, ByteString.EMPTY);
  }

  /**
   * Create a log entry with data for testing.
   */
  public static LogEntry makeEntry(long index, long term, ByteString data) {
    return LogEntry.newBuilder()
        .setIndex(index)
        .setTerm(term)
        .setData(data)
        .build();
  }
}
