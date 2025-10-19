package org.jraft.core;

import org.jraft.rpc.LogEntry;

public interface FiniteStateMachine {
  public record ApplyResult(long index, boolean ok, byte[] value, boolean dedupHit) {};

  ApplyResult apply(LogEntry e);

}
