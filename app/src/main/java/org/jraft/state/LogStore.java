package org.jraft.state;

import java.util.List;
import org.jraft.rpc.LogEntry;

public interface LogStore {
  long lastIndex();
  long termAt(long index);
  void append(List<LogEntry> entries);
  void truncateFrom(long index);
  LogEntry entryAt(long index);
}
