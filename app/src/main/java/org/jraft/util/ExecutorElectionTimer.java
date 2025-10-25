package org.jraft.util;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ThreadLocalRandom;

public final class ExecutorElectionTimer implements ElectionTimer {
  private final ScheduledExecutorService ses;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile ScheduledFuture<?> future;
  private volatile Runnable onTimeout;
  private volatile long minMs, maxMs;

  private final AtomicLong generation = new AtomicLong(0);

  public ExecutorElectionTimer(String threadName) {
    this.ses = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, threadName);
      t.setDaemon(true);
      return t;
    });
  }

  public synchronized void start(long minTimeoutMs, long maxTimeoutMs, Runnable onTimeout) {
    if (running.get()) return;
    if (minTimeoutMs <= 0 || maxTimeoutMs < minTimeoutMs) {
      throw new IllegalArgumentException("Bad election timeout bounds");
    }
    this.minMs = minTimeoutMs;
    this.maxMs = maxTimeoutMs;
    this.onTimeout = onTimeout;
    running.set(true);
    scheduleNext();
  }

  public synchronized void reset() {
    if (!running.get()) return;
    cancelCurrent();
    scheduleNext();
  }

  public synchronized void stop() {
    if (!running.get()) return;
    cancelCurrent();
    running.set(false);
  }

  public boolean isRunning() { return running.get(); }

  private void scheduleNext() {
    long gen = generation.incrementAndGet();
    long delay = jitter(minMs, maxMs);
    future = ses.schedule(() -> {
      if (running.get() && generation.get() == gen) {
        Runnable cb = onTimeout;
        if (cb != null) cb.run();
      }
    }, delay, TimeUnit.MILLISECONDS);
  }

  private void cancelCurrent() {
    ScheduledFuture<?> f = future;
    if (f != null) f.cancel(false);
    generation.incrementAndGet();
  }

  private static long jitter(long minMs, long maxMs) {
    if (minMs == maxMs) return minMs;
    return ThreadLocalRandom.current().nextLong(minMs, maxMs + 1);
  }

  public void shutdown() { ses.shutdownNow(); }
}
