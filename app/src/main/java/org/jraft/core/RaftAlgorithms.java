package org.jraft.core;

import java.util.List;

import org.jraft.rpc.LogEntry;
import org.jraft.state.LogStore;

public final class RaftAlgorithms {
  private RaftAlgorithms() {}

  // raft spec 5.4.1 candidate is at least as up-to-date as local
  public static boolean isCandidateUpToDate(LogStore log, long candidateLastTerm, long candidateLastIndex) {
    long localIndex = log.lastIndex();
    long localTerm = log.termAt(localIndex);
    if (candidateLastTerm > localTerm) return true;
    if (candidateLastTerm < localTerm) return false;
    return candidateLastIndex >= localIndex;
  }

  public record ApplyResult(boolean accepted, long lastNewIndex) {}

  public static ApplyResult applyLogPatch(
    LogStore log, long prevIndex, long prevTerm, List<LogEntry> entries) {
      if (log.termAt(prevIndex) != prevTerm)
        return new ApplyResult(false, prevIndex);
      if (entries.isEmpty())
        return new ApplyResult(true, prevIndex);
      if (entries.get(0).getIndex() != prevIndex + 1)
        return new ApplyResult(false, prevIndex);

      long lastNew = prevIndex;
      int k = 0;
      while (k < entries.size()) {
        LogEntry e = entries.get(k);
        long i = e.getIndex();
        long localTermAtI = log.termAt(i);
        
        if (localTermAtI == 0) {
          // beyond current end: append all remaining and finish
          log.append(entries.subList(k, entries.size()));
          lastNew = log.lastIndex();
          return new ApplyResult(true, lastNew);
        } else if (localTermAtI == e.getTerm()) {
            // already have identical entry at i
            lastNew = Math.max(lastNew, i);
            k++;
        } else {
            // first conflict: nuke suffix from i and append remainder
            log.truncateFrom(i);
            log.append(entries.subList(k, entries.size()));
            lastNew = log.lastIndex();
            return new ApplyResult(true, lastNew);
        }

        // if we got here, all entries matched (nothing appended)
      }
    return new ApplyResult(true, lastNew);
    }

}
