package org.jraft.util;

import java.util.concurrent.*;

import org.jraft.core.RepeatingTask;

public final class ExecutorRepeatingTask implements RepeatingTask {
  private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(r ->
      new Thread(r, "raft-heartbeats"));
  private ScheduledFuture<?> future;

  @Override public synchronized void start(Runnable task, long periodMillis) {
    if (future != null && !future.isCancelled() && !future.isDone()) return;
    future = ses.scheduleAtFixedRate(task, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
  }

  @Override public synchronized void stop() {
    if (future != null) future.cancel(false);
    future = null;
  }

  @Override public synchronized boolean isRunning() {
    return future != null && !future.isCancelled() && !future.isDone();
  }
}
