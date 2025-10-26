package org.jraft.state;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.jraft.rpc.LogEntry;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * file-based persistent log storage using write-ahead log pattern.
 *
 * storage format:
 *   - multiple segment files: wal-0000001.log, wal-0000002.log, etc.
 *   - each segment: [4 byte length][protobuf entry][4 byte length][protobuf entry]...
 *   - in-memory index maps log index -> (segment number, file offset)
 *   - automatically rotates segments when they exceed segmentSize
 *
 * crash recovery:
 *   - on startup, scans all segment files and rebuilds in-memory index
 *   - handles partial writes (from crashes) by stopping at first corrupt entry
 *
 * thread safety: all public methods are synchronized for simplicity
 */
public class FileLogStore implements LogStore {
  private final Path logDir;
  private final int segmentSize;

  private FileOutputStream currentSegment;
  private DataOutputStream currentWriter;
  private int currentSegmentNumber = 1;
  private long currentSegmentStartOffset = 0;  // byte offset where current segment starts writing

  // in-memory index for fast lookup: log index -> file location
  private final Map<Long, FileLocation> index = new HashMap<>();

  // cache for fast queries without disk access
  private long lastIndex = 0;
  private long lastTerm = 0;

  // tracks where each segment starts in terms of log indices
  private final Map<Integer, Long> segmentFirstIndex = new HashMap<>();

  /**
   * internal record tracking where an entry lives on disk
   */
  private record FileLocation(int segmentNumber, long offset) {}

  public FileLogStore(Path logDir) throws IOException {
    this(logDir, 10 * 1024 * 1024);  // default 10MB segments
  }

  public FileLogStore(Path logDir, int segmentSize) throws IOException {
    this.logDir = logDir;
    this.segmentSize = segmentSize;
    Files.createDirectories(logDir);

    // if existing segments exist, rebuild state from disk
    recoverFromDisk();
  }

  @Override
  public synchronized long lastIndex() {
    return lastIndex;
  }

  @Override
  public synchronized long termAt(long index) {
    if (index == 0) return 0;
    if (index > lastIndex) return 0;
    if (index == lastIndex) return lastTerm;  // fast path

    // need to load from disk
    LogEntry entry = entryAt(index);
    return entry == null ? 0 : entry.getTerm();
  }

  @Override
  public synchronized void append(List<LogEntry> entries) {
    if (entries.isEmpty()) return;

    try {
      for (LogEntry entry : entries) {
        // sanity check: entries should be contiguous
        if (entry.getIndex() != lastIndex + 1) {
          throw new IllegalArgumentException(
            "non-contiguous append: expected index " + (lastIndex + 1) +
            " but got " + entry.getIndex());
        }

        // check if we need to rotate to a new segment file
        if (shouldRotateSegment()) {
          rotateSegment();
        }

        // serialize entry to bytes
        byte[] data = entry.toByteArray();
        int length = data.length;

        // record current position before writing
        long offset = currentSegmentStartOffset;
        index.put(entry.getIndex(), new FileLocation(currentSegmentNumber, offset));

        // write format: [4-byte length][protobuf data]
        currentWriter.writeInt(length);
        currentWriter.write(data);

        // update tracking
        currentSegmentStartOffset += 4 + length;
        lastIndex = entry.getIndex();
        lastTerm = entry.getTerm();
      }

      // critical: force data to disk for durability
      // without this, data might only be in OS buffer and lost on crash
      currentWriter.flush();
      currentSegment.getFD().sync();
    } catch (IOException e) {
      throw new RuntimeException("failed to append entries to log", e);
    }
  }

  @Override
  public synchronized LogEntry entryAt(long index) {
    if (index <= 0 || index > lastIndex) return null;

    FileLocation loc = this.index.get(index);
    if (loc == null) return null;

    // open the segment file and seek to the right location
    Path segmentPath = segmentPath(loc.segmentNumber);
    try (RandomAccessFile raf = new RandomAccessFile(segmentPath.toFile(), "r")) {
      raf.seek(loc.offset);

      // read length prefix
      int length = raf.readInt();

      // read protobuf data
      byte[] data = new byte[length];
      raf.readFully(data);

      // deserialize
      return LogEntry.parseFrom(data);
    } catch (IOException e) {
      throw new RuntimeException("failed to read entry at index " + index, e);
    }
  }

  @Override
  public synchronized void truncateFrom(long index) {
    try {
      if (index <= 0) {
        // truncate everything - delete all segments and start fresh
        deleteAllSegments();
        currentSegmentNumber = 1;
        currentSegmentStartOffset = 0;
        lastIndex = 0;
        lastTerm = 0;
        this.index.clear();
        segmentFirstIndex.clear();

        // create new empty segment
        openNewSegment(1);
        return;
      }

      if (index > lastIndex) {
        // nothing to truncate
        return;
      }

      // remove all index entries >= index
      List<Long> toRemove = this.index.keySet().stream()
        .filter(i -> i >= index)
        .collect(Collectors.toList());
      toRemove.forEach(this.index::remove);

      // find the segment containing the new last entry (index - 1)
      if (index == 1) {
        // truncating from index 1 means empty log
        deleteAllSegments();
        currentSegmentNumber = 1;
        currentSegmentStartOffset = 0;
        lastIndex = 0;
        lastTerm = 0;
        segmentFirstIndex.clear();
        openNewSegment(1);
        return;
      }

      FileLocation newLastLoc = this.index.get(index - 1);
      if (newLastLoc == null) {
        throw new IllegalStateException("index corrupted: missing entry for " + (index - 1));
      }

      // close current segment
      closeCurrentSegment();

      // delete all segments after the one containing our new last entry
      deleteSegmentsAfter(newLastLoc.segmentNumber);

      // truncate the segment file containing the new last entry
      // we need to truncate at the position AFTER the last entry we're keeping
      truncateSegmentAfter(newLastLoc);

      // reopen the segment for appending
      currentSegmentNumber = newLastLoc.segmentNumber;
      openSegmentForAppending(currentSegmentNumber);

      // update cache
      LogEntry newLastEntry = entryAt(index - 1);
      lastIndex = newLastEntry.getIndex();
      lastTerm = newLastEntry.getTerm();

      // recalculate current offset by reading through the segment
      currentSegmentStartOffset = calculateSegmentEndOffset(currentSegmentNumber);
    } catch (IOException e) {
      throw new RuntimeException("failed to truncate log from index " + index, e);
    }
  }

  // --- private helper methods ---

  /**
   * recover state from existing segment files on disk
   */
  private void recoverFromDisk() throws IOException {
    List<Path> segments = listSegmentsSorted();

    if (segments.isEmpty()) {
      // fresh start - create initial segment
      openNewSegment(1);
      segmentFirstIndex.put(1, 1L);
      return;
    }

    // rebuild index by reading all segments sequentially
    for (Path segmentPath : segments) {
      int segNum = extractSegmentNumber(segmentPath);
      long segStartIndex = lastIndex + 1;
      segmentFirstIndex.put(segNum, segStartIndex);

      try (DataInputStream dis = new DataInputStream(new FileInputStream(segmentPath.toFile()))) {
        long currentOffset = 0;

        while (dis.available() > 0) {
          // try to read length
          int length;
          try {
            length = dis.readInt();
          } catch (Exception e) {
            // partial write at end of file (crash during write) - stop here
            System.err.println("recovery: found partial write in " + segmentPath.getFileName() +
                             ", truncating");
            break;
          }

          // try to read entry data
          byte[] data = new byte[length];
          int bytesRead = dis.read(data);
          if (bytesRead < length) {
            // partial write - stop here
            System.err.println("recovery: found incomplete entry in " + segmentPath.getFileName() +
                             ", truncating");
            break;
          }

          // try to parse protobuf
          LogEntry entry;
          try {
            entry = LogEntry.parseFrom(data);
          } catch (InvalidProtocolBufferException e) {
            // corrupt entry - stop recovery here
            System.err.println("recovery: found corrupt protobuf in " + segmentPath.getFileName() +
                             ", truncating at index " + lastIndex);
            break;
          }

          // successfully read entry - add to index
          index.put(entry.getIndex(), new FileLocation(segNum, currentOffset));
          lastIndex = entry.getIndex();
          lastTerm = entry.getTerm();

          currentOffset += 4 + length;
        }
      }
    }

    // open the last segment for appending
    if (!segments.isEmpty()) {
      currentSegmentNumber = extractSegmentNumber(segments.get(segments.size() - 1));
      openSegmentForAppending(currentSegmentNumber);
      currentSegmentStartOffset = calculateSegmentEndOffset(currentSegmentNumber);
    } else {
      openNewSegment(1);
      segmentFirstIndex.put(1, 1L);
    }
  }

  /**
   * check if current segment has grown too large
   */
  private boolean shouldRotateSegment() {
    return currentSegmentStartOffset >= segmentSize;
  }

  /**
   * close current segment and open a new one
   */
  private void rotateSegment() throws IOException {
    closeCurrentSegment();
    currentSegmentNumber++;
    currentSegmentStartOffset = 0;
    openNewSegment(currentSegmentNumber);
    segmentFirstIndex.put(currentSegmentNumber, lastIndex + 1);
  }

  /**
   * open a brand new segment file for writing
   */
  private void openNewSegment(int segNum) throws IOException {
    Path path = segmentPath(segNum);
    currentSegment = new FileOutputStream(path.toFile());
    currentWriter = new DataOutputStream(currentSegment);
  }

  /**
   * open existing segment for appending
   */
  private void openSegmentForAppending(int segNum) throws IOException {
    Path path = segmentPath(segNum);
    currentSegment = new FileOutputStream(path.toFile(), true);  // append mode
    currentWriter = new DataOutputStream(currentSegment);
  }

  /**
   * close the current segment writer
   */
  private void closeCurrentSegment() throws IOException {
    if (currentWriter != null) {
      currentWriter.close();
      currentWriter = null;
    }
    if (currentSegment != null) {
      currentSegment.close();
      currentSegment = null;
    }
  }

  /**
   * delete all segment files
   */
  private void deleteAllSegments() throws IOException {
    closeCurrentSegment();
    List<Path> segments = listSegmentsSorted();
    for (Path seg : segments) {
      Files.deleteIfExists(seg);
    }
  }

  /**
   * delete all segment files with number > segNum
   */
  private void deleteSegmentsAfter(int segNum) throws IOException {
    List<Path> segments = listSegmentsSorted();
    for (Path seg : segments) {
      int num = extractSegmentNumber(seg);
      if (num > segNum) {
        Files.deleteIfExists(seg);
      }
    }
  }

  /**
   * truncate a segment file after the given location
   */
  private void truncateSegmentAfter(FileLocation loc) throws IOException {
    Path path = segmentPath(loc.segmentNumber);

    // read the entry at this location to find where it ends
    try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
      raf.seek(loc.offset);
      int length = raf.readInt();
      long truncatePos = loc.offset + 4 + length;

      // truncate file at this position
      try (RandomAccessFile writer = new RandomAccessFile(path.toFile(), "rw")) {
        writer.setLength(truncatePos);
      }
    }
  }

  /**
   * calculate the end offset of a segment by scanning through it
   */
  private long calculateSegmentEndOffset(int segNum) throws IOException {
    Path path = segmentPath(segNum);
    long offset = 0;

    try (DataInputStream dis = new DataInputStream(new FileInputStream(path.toFile()))) {
      while (dis.available() > 0) {
        int length = dis.readInt();
        dis.skipBytes(length);
        offset += 4 + length;
      }
    }

    return offset;
  }

  /**
   * get path for a segment file
   */
  private Path segmentPath(int segNum) {
    return logDir.resolve(String.format("wal-%07d.log", segNum));
  }

  /**
   * list all segment files in sorted order
   */
  private List<Path> listSegmentsSorted() throws IOException {
    if (!Files.exists(logDir)) return List.of();
    System.out.println("logDir = " + logDir.toAbsolutePath());
    System.out.println("Exists: " + Files.exists(logDir));
    System.out.println("Is directory: " + Files.isDirectory(logDir));


    var out = Files.list(logDir).collect(Collectors.toList());;
      // .filter(p -> p.getFileName().toString().startsWith("wal-"))
      // .filter(p -> p.getFileName().toString().endsWith(".log"))
      // .sorted()
      // .collect(Collectors.toList());
  
    return out;
  }

  /**
   * extract segment number from filename like "wal-0000001.log"
   */
  private int extractSegmentNumber(Path path) {
    String name = path.getFileName().toString();
    String numPart = name.substring(4, 11);  // "wal-0000001.log" -> "0000001"
    return Integer.parseInt(numPart);
  }

  /**
   * close and cleanup resources
   */
  public synchronized void close() throws IOException {
    closeCurrentSegment();
  }
}
