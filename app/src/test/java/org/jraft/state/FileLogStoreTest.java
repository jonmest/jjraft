package org.jraft.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import org.jraft.rpc.LogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.protobuf.ByteString;

/**
 * comprehensive tests for FileLogStore persistence layer
 */
public class FileLogStoreTest {

  @TempDir
  Path tempDir;

  private Path logDir;

  @BeforeEach
  void setup() {
    logDir = tempDir.resolve("log");
  }

  @AfterEach
  void cleanup() throws IOException {
    // clean up any test files
    if (Files.exists(logDir)) {
      Files.walk(logDir)
        .sorted(Comparator.reverseOrder())
        .forEach(path -> {
          try { Files.deleteIfExists(path); } catch (IOException e) {}
        });
    }
  }

  // --- basic operations tests ---

  @Test
  void testFreshLogIsEmpty() throws IOException {
    FileLogStore log = new FileLogStore(logDir);

    assertEquals(0, log.lastIndex());
    assertEquals(0, log.termAt(0));
    assertEquals(0, log.termAt(1));
    assertNull(log.entryAt(1));

    log.close();
  }

  @Test
  void testAppendSingleEntry() throws IOException {
    FileLogStore log = new FileLogStore(logDir);

    LogEntry e1 = makeEntry(1, 1, "cmd1");
    log.append(List.of(e1));

    assertEquals(1, log.lastIndex());
    assertEquals(1, log.termAt(1));

    LogEntry retrieved = log.entryAt(1);
    assertNotNull(retrieved);
    assertEquals(1, retrieved.getIndex());
    assertEquals(1, retrieved.getTerm());
    assertEquals(ByteString.copyFromUtf8("cmd1"), retrieved.getData());

    log.close();
  }

  @Test
  void testAppendMultipleEntries() throws IOException {
    FileLogStore log = new FileLogStore(logDir);

    LogEntry e1 = makeEntry(1, 1, "cmd1");
    LogEntry e2 = makeEntry(2, 1, "cmd2");
    LogEntry e3 = makeEntry(3, 2, "cmd3");

    log.append(List.of(e1, e2, e3));

    assertEquals(3, log.lastIndex());
    assertEquals(1, log.termAt(1));
    assertEquals(1, log.termAt(2));
    assertEquals(2, log.termAt(3));

    assertEquals("cmd1", log.entryAt(1).getData().toStringUtf8());
    assertEquals("cmd2", log.entryAt(2).getData().toStringUtf8());
    assertEquals("cmd3", log.entryAt(3).getData().toStringUtf8());

    log.close();
  }

  @Test
  void testAppendInBatches() throws IOException {
    FileLogStore log = new FileLogStore(logDir);

    // first batch
    log.append(List.of(makeEntry(1, 1, "a"), makeEntry(2, 1, "b")));
    assertEquals(2, log.lastIndex());

    // second batch
    log.append(List.of(makeEntry(3, 2, "c"), makeEntry(4, 2, "d")));
    assertEquals(4, log.lastIndex());

    // verify all entries readable
    assertEquals("a", log.entryAt(1).getData().toStringUtf8());
    assertEquals("b", log.entryAt(2).getData().toStringUtf8());
    assertEquals("c", log.entryAt(3).getData().toStringUtf8());
    assertEquals("d", log.entryAt(4).getData().toStringUtf8());

    log.close();
  }

  @Test
  void testNonContiguousAppendThrows() throws IOException {
    FileLogStore log = new FileLogStore(logDir);

    log.append(List.of(makeEntry(1, 1, "cmd1")));

    // try to append entry 3 when we only have entry 1
    assertThrows(IllegalArgumentException.class, () -> {
      log.append(List.of(makeEntry(3, 1, "cmd3")));
    });

    log.close();
  }

  // --- crash recovery tests ---

  @Test
  void testCrashRecoverySimple() throws IOException {
    // phase 1: write entries
    FileLogStore log1 = new FileLogStore(logDir);
    log1.append(List.of(
      makeEntry(1, 1, "cmd1"),
      makeEntry(2, 1, "cmd2"),
      makeEntry(3, 2, "cmd3")
    ));
    log1.close();

    // phase 2: simulate crash - create new instance (should recover from disk)
    FileLogStore log2 = new FileLogStore(logDir);

    assertEquals(3, log2.lastIndex());
    assertEquals(1, log2.termAt(1));
    assertEquals(1, log2.termAt(2));
    assertEquals(2, log2.termAt(3));

    assertEquals("cmd1", log2.entryAt(1).getData().toStringUtf8());
    assertEquals("cmd2", log2.entryAt(2).getData().toStringUtf8());
    assertEquals("cmd3", log2.entryAt(3).getData().toStringUtf8());

    log2.close();
  }

  @Test
  void testCrashRecoveryThenAppendMore() throws IOException {
    // write entries
    FileLogStore log1 = new FileLogStore(logDir);
    log1.append(List.of(makeEntry(1, 1, "a"), makeEntry(2, 1, "b")));
    log1.close();

    // recover and append more
    FileLogStore log2 = new FileLogStore(logDir);
    assertEquals(2, log2.lastIndex());

    log2.append(List.of(makeEntry(3, 2, "c"), makeEntry(4, 2, "d")));
    assertEquals(4, log2.lastIndex());

    log2.close();

    // recover again and verify all entries
    FileLogStore log3 = new FileLogStore(logDir);
    assertEquals(4, log3.lastIndex());
    assertEquals("a", log3.entryAt(1).getData().toStringUtf8());
    assertEquals("b", log3.entryAt(2).getData().toStringUtf8());
    assertEquals("c", log3.entryAt(3).getData().toStringUtf8());
    assertEquals("d", log3.entryAt(4).getData().toStringUtf8());

    log3.close();
  }

  @Test
  void testRecoveryWithLargeEntries() throws IOException {
    // write some large entries (test with bigger data)
    FileLogStore log1 = new FileLogStore(logDir);

    String largeData = "x".repeat(10000);  // 10KB entry
    log1.append(List.of(
      makeEntry(1, 1, largeData),
      makeEntry(2, 1, largeData),
      makeEntry(3, 2, largeData)
    ));
    log1.close();

    // recover
    FileLogStore log2 = new FileLogStore(logDir);
    assertEquals(3, log2.lastIndex());
    assertEquals(10000, log2.entryAt(1).getData().size());
    assertEquals(10000, log2.entryAt(2).getData().size());
    assertEquals(10000, log2.entryAt(3).getData().size());

    log2.close();
  }

  // --- truncation tests ---

  @Test
  void testTruncateFromMiddle() throws IOException {
    FileLogStore log = new FileLogStore(logDir);

    log.append(List.of(
      makeEntry(1, 1, "a"),
      makeEntry(2, 1, "b"),
      makeEntry(3, 2, "c"),
      makeEntry(4, 2, "d")
    ));

    // truncate from index 3 (removes 3 and 4)
    log.truncateFrom(3);

    assertEquals(2, log.lastIndex());
    assertEquals(1, log.termAt(2));
    assertNull(log.entryAt(3));
    assertNull(log.entryAt(4));

    // can append new entries after truncation
    log.append(List.of(makeEntry(3, 3, "new3")));
    assertEquals(3, log.lastIndex());
    assertEquals("new3", log.entryAt(3).getData().toStringUtf8());

    log.close();
  }

  @Test
  void testTruncateFromBeginning() throws IOException {
    FileLogStore log = new FileLogStore(logDir);

    log.append(List.of(
      makeEntry(1, 1, "a"),
      makeEntry(2, 1, "b"),
      makeEntry(3, 2, "c")
    ));

    // truncate from index 1 (removes everything)
    log.truncateFrom(1);

    assertEquals(0, log.lastIndex());
    assertNull(log.entryAt(1));

    // can start fresh after truncating everything
    log.append(List.of(makeEntry(1, 5, "fresh")));
    assertEquals(1, log.lastIndex());
    assertEquals(5, log.termAt(1));

    log.close();
  }

  @Test
  void testTruncateAll() throws IOException {
    FileLogStore log = new FileLogStore(logDir);

    log.append(List.of(makeEntry(1, 1, "a"), makeEntry(2, 1, "b")));

    // truncate with index <= 0 removes everything
    log.truncateFrom(0);

    assertEquals(0, log.lastIndex());
    assertNull(log.entryAt(1));

    log.close();
  }

  @Test
  void testTruncateBeyondEnd() throws IOException {
    FileLogStore log = new FileLogStore(logDir);

    log.append(List.of(makeEntry(1, 1, "a"), makeEntry(2, 1, "b")));

    // truncate from index beyond last entry (no-op)
    log.truncateFrom(10);

    assertEquals(2, log.lastIndex());
    assertEquals("a", log.entryAt(1).getData().toStringUtf8());
    assertEquals("b", log.entryAt(2).getData().toStringUtf8());

    log.close();
  }

  @Test
  void testTruncateThenRecover() throws IOException {
    // write entries
    FileLogStore log1 = new FileLogStore(logDir);
    log1.append(List.of(
      makeEntry(1, 1, "a"),
      makeEntry(2, 1, "b"),
      makeEntry(3, 2, "c"),
      makeEntry(4, 2, "d")
    ));

    // truncate
    log1.truncateFrom(3);
    assertEquals(2, log1.lastIndex());
    log1.close();

    // recover - truncation should persist
    FileLogStore log2 = new FileLogStore(logDir);
    assertEquals(2, log2.lastIndex());
    assertNull(log2.entryAt(3));
    assertEquals("a", log2.entryAt(1).getData().toStringUtf8());
    assertEquals("b", log2.entryAt(2).getData().toStringUtf8());

    log2.close();
  }

  // --- segment rotation tests ---

  @Test
  void testSegmentRotation() throws IOException {
    // create log with small segment size to force rotation
    int smallSegmentSize = 1024;  // 1KB segments
    FileLogStore log = new FileLogStore(logDir, smallSegmentSize);

    // write entries that exceed segment size
    String largeData = "x".repeat(500);  // 500 bytes per entry
    for (int i = 1; i <= 10; i++) {
      log.append(List.of(makeEntry(i, 1, largeData + i)));
    }

    assertEquals(10, log.lastIndex());

    // should have created multiple segment files
    long segmentCount = Files.list(logDir)
      .filter(p -> p.getFileName().toString().startsWith("wal-"))
      .count();

    assertTrue(segmentCount > 1, "should have created multiple segments");

    // all entries should still be readable
    for (int i = 1; i <= 10; i++) {
      LogEntry e = log.entryAt(i);
      assertNotNull(e);
      assertEquals(i, e.getIndex());
      assertTrue(e.getData().toStringUtf8().contains(largeData));
    }

    log.close();
  }

  @Test
  void testRecoveryAcrossMultipleSegments() throws IOException {
    // write with small segments
    int smallSegmentSize = 1024;
    FileLogStore log1 = new FileLogStore(logDir, smallSegmentSize);

    String data = "x".repeat(500);
    for (int i = 1; i <= 10; i++) {
      log1.append(List.of(makeEntry(i, 1, data)));
    }
    log1.close();

    // recover
    FileLogStore log2 = new FileLogStore(logDir, smallSegmentSize);

    assertEquals(10, log2.lastIndex());

    // verify all entries recovered correctly
    for (int i = 1; i <= 10; i++) {
      assertNotNull(log2.entryAt(i));
      assertEquals(i, log2.entryAt(i).getIndex());
    }

    log2.close();
  }

  @Test
  void testTruncateAcrossSegments() throws IOException {
    // write with small segments
    int smallSegmentSize = 1024;
    FileLogStore log = new FileLogStore(logDir, smallSegmentSize);

    String data = "x".repeat(500);
    for (int i = 1; i <= 10; i++) {
      log.append(List.of(makeEntry(i, 1, data)));
    }

    // truncate from middle (should remove some segments)
    log.truncateFrom(5);

    assertEquals(4, log.lastIndex());
    assertNull(log.entryAt(5));
    assertNotNull(log.entryAt(4));

    log.close();
  }

  // --- edge case tests ---

  @Test
  void testEmptyAppend() throws IOException {
    FileLogStore log = new FileLogStore(logDir);

    // appending empty list should be no-op
    log.append(List.of());

    assertEquals(0, log.lastIndex());

    log.close();
  }

  @Test
  void testZeroLengthData() throws IOException {
    FileLogStore log = new FileLogStore(logDir);

    LogEntry empty = makeEntry(1, 1, "");
    log.append(List.of(empty));

    assertEquals(1, log.lastIndex());

    LogEntry retrieved = log.entryAt(1);
    assertNotNull(retrieved);
    assertEquals(ByteString.EMPTY, retrieved.getData());

    log.close();
  }

  @Test
  void testQueryOutOfBounds() throws IOException {
    FileLogStore log = new FileLogStore(logDir);

    log.append(List.of(makeEntry(1, 1, "a"), makeEntry(2, 1, "b")));

    // query before start
    assertNull(log.entryAt(0));
    assertEquals(0, log.termAt(0));

    // query after end
    assertNull(log.entryAt(3));
    assertEquals(0, log.termAt(3));

    // negative index
    assertNull(log.entryAt(-1));

    log.close();
  }

  @Test
  void testMultipleCloseCalls() throws IOException {
    FileLogStore log = new FileLogStore(logDir);
    log.append(List.of(makeEntry(1, 1, "a")));

    // closing multiple times should be safe
    log.close();
    log.close();
    log.close();
  }

  @Test
  void testVeryLargeIndex() throws IOException {
    // test that we can handle large index numbers
    // (in practice this shouldn't happen due to snapshots, but test anyway)
    FileLogStore log = new FileLogStore(logDir);

    // simulate appending after a snapshot at high index
    // we can't actually start from a high index without cheating,
    // so just test that large indices work correctly in general
    for (int i = 1; i <= 1000; i++) {
      log.append(List.of(makeEntry(i, 1, "cmd" + i)));
    }

    assertEquals(1000, log.lastIndex());
    assertEquals("cmd500", log.entryAt(500).getData().toStringUtf8());

    log.close();
  }

  // --- raft-specific scenario tests ---

  @Test
  void testFollowerConflictResolution() throws IOException {
    // simulate raft conflict resolution scenario:
    // follower has [1,1] [2,1] [3,2]
    // leader sends [3,3] which conflicts
    // follower must truncate from 3 and append new entry

    FileLogStore log = new FileLogStore(logDir);

    log.append(List.of(
      makeEntry(1, 1, "a"),
      makeEntry(2, 1, "b"),
      makeEntry(3, 2, "conflict")
    ));

    // truncate conflicting entries
    log.truncateFrom(3);

    // append leader's version
    log.append(List.of(makeEntry(3, 3, "leader-version")));

    assertEquals(3, log.lastIndex());
    assertEquals(3, log.termAt(3));
    assertEquals("leader-version", log.entryAt(3).getData().toStringUtf8());

    log.close();
  }

  @Test
  void testFollowerConflictResolutionPersists() throws IOException {
    // same as above but verify it persists across restart

    FileLogStore log1 = new FileLogStore(logDir);
    log1.append(List.of(
      makeEntry(1, 1, "a"),
      makeEntry(2, 1, "b"),
      makeEntry(3, 2, "conflict")
    ));
    log1.truncateFrom(3);
    log1.append(List.of(makeEntry(3, 3, "leader-version")));
    log1.close();

    // verify after recovery
    FileLogStore log2 = new FileLogStore(logDir);
    assertEquals(3, log2.lastIndex());
    assertEquals(3, log2.termAt(3));
    assertEquals("leader-version", log2.entryAt(3).getData().toStringUtf8());

    log2.close();
  }

  @Test
  void testLeaderAppendEntries() throws IOException {
    // simulate leader appending client commands
    FileLogStore log = new FileLogStore(logDir);

    // leader in term 5 receives commands
    for (int i = 1; i <= 5; i++) {
      log.append(List.of(makeEntry(i, 5, "client-cmd-" + i)));
    }

    assertEquals(5, log.lastIndex());

    // all entries have same term
    for (int i = 1; i <= 5; i++) {
      assertEquals(5, log.termAt(i));
    }

    log.close();
  }

  // --- helper methods ---

  private LogEntry makeEntry(long index, long term, String data) {
    return LogEntry.newBuilder()
      .setIndex(index)
      .setTerm(term)
      .setData(ByteString.copyFromUtf8(data))
      .build();
  }
}
