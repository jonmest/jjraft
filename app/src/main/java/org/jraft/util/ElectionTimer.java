package org.jraft.util;

public interface ElectionTimer {
  void start(long minTimeoutMs, long maxTimeoutMs, Runnable onTimeout);
  void stop();
  void reset();
  boolean isRunning();
}
