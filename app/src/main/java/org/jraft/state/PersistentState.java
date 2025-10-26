package org.jraft.state;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import com.google.gson.Gson;

public class PersistentState {
  private final Path metadataFile;
  private long term;
  private String votedFor;

  public PersistentState(Path metadataFile, long term, String votedFor) {
    this.metadataFile = metadataFile;
    this.term = term;
    this.votedFor = votedFor;
  }

  public static PersistentState load(Path dataDir) throws IOException {
    Path metadataFile = dataDir.resolve("metadata.json");
    if (!Files.exists(metadataFile)) {
      return new PersistentState(metadataFile, 0, null);
    }

    try {
      String json = Files.readString(metadataFile);
      Record r = new Gson().fromJson(json, Record.class);
      return new PersistentState(metadataFile, r.term, r.votedFor);
    } catch (IOException e) {
      System.err.println("Failed to read persisted metadata.json.");
      throw e;
    }  
  }

  public void save(long term, String votedFor) throws IOException {
    this.term = term;
    this.votedFor = votedFor;
    
    // atomic write pattern
    Path tempFile = metadataFile.getParent().resolve("metadata.tmp");
    String json = createJson(term, votedFor);
    Files.writeString(tempFile, json);

    // force write to disk now
    try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.WRITE)) {
      channel.force(true);
    } catch (IOException e) {
        try { Files.deleteIfExists(tempFile); } catch (IOException ignored) {}
        throw e;
    }

    // atomic rename - if crash happens before name the old file survives.
    // if crash happens after rename, the new file survives.
    try {
      Files.move(tempFile, metadataFile, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException e) {
        // at least replace as a fallback; not ideal, but better than failing silently
        Files.move(tempFile, metadataFile, StandardCopyOption.REPLACE_EXISTING);
    } finally {
        // If move failed, clean temp
        try { Files.deleteIfExists(tempFile); } catch (IOException ignored) {}
    }

    // note: syncing parent directory ensures rename is durable on disk
    // however, FileChannel.open() doesn't work on directories on most systems
    // the proper way is platform-specific (fsync on directory fd in Linux)
    // for this implementation we skip it - the atomic rename still provides
    // good crash safety, just not 100% guaranteed durable on power loss
    // in production you'd use JNI or accept this trade-off
  }

  public long getCurrentTerm(){ return this.term; }
  public String getVotedFor() { return this.votedFor; }

  private static record Record(long term, String votedFor){};

  private String createJson(long term, String votedFor) {
    var rec = new Record(term, votedFor);
    Gson gson = new Gson();
    return gson.toJson(rec);
  }
}
