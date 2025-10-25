package org.jraft.util;

public final class FakeElectionTimer implements ElectionTimer {
  private boolean running;
  private Runnable onTimeout;

  public int resetCount = 0;

  @Override public void start(long minTimeoutMs, long maxTimeoutMs, Runnable onTimeout) {
    this.running = true;
    this.onTimeout = onTimeout;
  }
  @Override public void stop() { running = false; }
  @Override public void reset() { if (running) resetCount++; }
  @Override public boolean isRunning() { return running; }

  public void elapse() { if (running && onTimeout != null) onTimeout.run(); }
}
