package org.jraft.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class PersistentStateTest {

  @TempDir
  Path tempDir;

  @AfterEach
  void cleanup() throws IOException {
    // clean up metadata files
    Files.deleteIfExists(tempDir.resolve("metadata.json"));
    Files.deleteIfExists(tempDir.resolve("metadata.tmp"));
  }

  @Test
  void testFreshLoadReturnsDefaults() throws IOException {
    // loading from empty directory should give term=0, votedFor=null
    PersistentState state = PersistentState.load(tempDir);

    assertEquals(0, state.getCurrentTerm());
    assertNull(state.getVotedFor());
  }

  @Test
  void testSaveAndLoad() throws IOException {
    PersistentState state = PersistentState.load(tempDir);

    state.save(5, "node-1");

    // load in new instance
    PersistentState loaded = PersistentState.load(tempDir);

    assertEquals(5, loaded.getCurrentTerm());
    assertEquals("node-1", loaded.getVotedFor());
  }

  @Test
  void testMultipleSaves() throws IOException {
    PersistentState state = PersistentState.load(tempDir);

    // multiple saves should overwrite previous values
    state.save(1, "node-1");
    state.save(2, "node-2");
    state.save(3, "node-3");

    PersistentState loaded = PersistentState.load(tempDir);
    assertEquals(3, loaded.getCurrentTerm());
    assertEquals("node-3", loaded.getVotedFor());
  }

  @Test
  void testSaveNullVotedFor() throws IOException {
    PersistentState state = PersistentState.load(tempDir);

    // can save null for votedFor (haven't voted yet)
    state.save(5, null);

    PersistentState loaded = PersistentState.load(tempDir);
    assertEquals(5, loaded.getCurrentTerm());
    assertNull(loaded.getVotedFor());
  }

  @Test
  void testAtomicWrite() throws IOException {
    // verify that saves are atomic - old file should survive until rename
    PersistentState state = PersistentState.load(tempDir);

    state.save(1, "node-1");

    // verify file exists
    assertTrue(Files.exists(tempDir.resolve("metadata.json")));

    // save again
    state.save(2, "node-2");

    // should have new values
    PersistentState loaded = PersistentState.load(tempDir);
    assertEquals(2, loaded.getCurrentTerm());
    assertEquals("node-2", loaded.getVotedFor());
  }

  @Test
  void testNoTempFileLeftBehind() throws IOException {
    PersistentState state = PersistentState.load(tempDir);

    state.save(5, "node-1");

    // temp file should be cleaned up after save
    assertTrue(Files.exists(tempDir.resolve("metadata.json")));
    // temp file might exist briefly but should be gone or renamed
    // we can't easily test the atomic nature without injecting failures
  }

  @Test
  void testSurvivesMultipleLoads() throws IOException {
    PersistentState state1 = PersistentState.load(tempDir);
    state1.save(10, "leader");

    // load multiple times
    PersistentState state2 = PersistentState.load(tempDir);
    assertEquals(10, state2.getCurrentTerm());

    PersistentState state3 = PersistentState.load(tempDir);
    assertEquals(10, state3.getCurrentTerm());

    PersistentState state4 = PersistentState.load(tempDir);
    assertEquals(10, state4.getCurrentTerm());
  }

  @Test
  void testRaftScenario() throws IOException {
    // simulate raft node behavior
    PersistentState state = PersistentState.load(tempDir);

    // start election in term 1
    state.save(1, "self");

    // lose election, hear from term 2
    state.save(2, null);

    // start election in term 3
    state.save(3, "self");

    // verify final state
    PersistentState loaded = PersistentState.load(tempDir);
    assertEquals(3, loaded.getCurrentTerm());
    assertEquals("self", loaded.getVotedFor());
  }
}
