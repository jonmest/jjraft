package org.jraft.core.util;

import org.jraft.core.RepeatingTask;

public final class FakeRepeatingTask implements RepeatingTask {
  private Runnable task;
  private boolean running;

  @Override public void start(Runnable task, long periodMillis) {
    this.task = task;
    this.running = true;
  }

  @Override public void stop() {
    this.running = false;
  }

  @Override public boolean isRunning() { return running; }

  public void tickOnce() {
    if (running && task != null) task.run();
  }
}
