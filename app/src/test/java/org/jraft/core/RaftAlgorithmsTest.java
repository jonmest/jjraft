package org.jraft.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.jraft.rpc.LogEntry;
import org.jraft.state.LogStore;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

final class RaftAlgorithmsTest {

    /** Minimal in-memory LogStore just for these tests. */
    static final class MemLog implements LogStore {
        // 1-based index; index 0 is "empty"
        final ArrayList<LogEntry> entries = new ArrayList<>();

        @Override public long lastIndex() {
            return entries.size();
        }

        @Override public long termAt(long index) {
            if (index <= 0 || index > entries.size()) return 0;
            return entries.get((int) index - 1).getTerm();
        }

        @Override public void append(List<LogEntry> batch) {
            if (batch.isEmpty()) return;
            long expected = lastIndex() + 1;
            if (batch.get(0).getIndex() != expected) {
                throw new IllegalStateException("append not contiguous: got " + batch.get(0).getIndex() + " expected " + expected);
            }
            entries.addAll(batch);
        }

        @Override public void truncateFrom(long index) {
            if (index <= 0) { entries.clear(); return; }
            while (entries.size() >= index) entries.remove(entries.size() - 1);
        }

        @Override public LogEntry entryAt(long index) {
            if (index <= 0 || index > entries.size()) return null;
            return entries.get((int) index - 1);
        }

        // helpers for setup
        void add(long index, long term) {
            entries.add(LogEntry.newBuilder()
                    .setIndex(index).setTerm(term).setData(ByteString.EMPTY).build());
        }
    }

    // ---------- Tests ----------

    @Test
    void isCandidateUpToDate_followsSpec() {
        var log = new MemLog();
        // terms: index 1..10 all term=3
        for (int i = 1; i <= 10; i++) log.add(i, 3);

        assertTrue(RaftAlgorithms.isCandidateUpToDate(log, 3, 10)); // equal
        assertFalse(RaftAlgorithms.isCandidateUpToDate(log, 3, 9)); // lower idx
        assertTrue(RaftAlgorithms.isCandidateUpToDate(log, 4, 1));  // higher term wins
        assertFalse(RaftAlgorithms.isCandidateUpToDate(log, 2, 100)); // lower term loses
    }

    @Test
    void applyLogPatch_rejectsOnPrevMismatch() {
        var log = new MemLog();
        // local: index 1..5 term=2
        for (int i = 1; i <= 5; i++) log.add(i, 2);

        var res = RaftAlgorithms.applyLogPatch(log, 5, /*prevTerm*/3, List.of());
        assertFalse(res.accepted());
        assertEquals(5, log.lastIndex()); // unchanged
        assertEquals(2, log.termAt(5));
    }

    @Test
    void applyLogPath_handleEmptyLogAndHeartbeat() {
        var log = new MemLog();
        var res = RaftAlgorithms.applyLogPatch(log, 0, 0, List.of());
        assertTrue(res.accepted());
        assertEquals(0, log.lastIndex());
        assertEquals(0, log.termAt(0));
    }

    @Test
    void applyLogPatch_appendsOnCleanMatch() {
        var log = new MemLog();
        for (int i = 1; i <= 5; i++) log.add(i, 2);

        var e6 = LogEntry.newBuilder().setIndex(6).setTerm(2).build();
        var e7 = LogEntry.newBuilder().setIndex(7).setTerm(2).build();

        var res = RaftAlgorithms.applyLogPatch(log, 5, 2, List.of(e6, e7));
        assertTrue(res.accepted());
        assertEquals(7, res.lastNewIndex());
        assertEquals(7, log.lastIndex());
        assertEquals(2, log.termAt(6));
        assertEquals(2, log.termAt(7));
    }

    @Test
    void applyLogPatch_truncatesAndReplacesOnConflict() {
        var log = new MemLog();
        // terms: [1,1,2,2,3,3,3] at indices 1..7
        log.add(1,1); log.add(2,1); log.add(3,2); log.add(4,2);
        log.add(5,3); log.add(6,3); log.add(7,3);

        var e5 = LogEntry.newBuilder().setIndex(5).setTerm(4).build();
        var e6 = LogEntry.newBuilder().setIndex(6).setTerm(4).build();

        var res = RaftAlgorithms.applyLogPatch(log, 4, 2, List.of(e5, e6));
        assertTrue(res.accepted());
        assertEquals(6, res.lastNewIndex());
        assertEquals(6, log.lastIndex());         // index 7 removed
        assertEquals(4, log.termAt(5));
        assertEquals(4, log.termAt(6));
    }
}
