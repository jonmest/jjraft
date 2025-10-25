package org.jraft.core;

public interface RepeatingTask {
  void start(Runnable task, long periodMillis);
  void stop();
  boolean isRunning();
}
