package org.jraft.kv;

import java.util.*;
import java.util.Arrays;

import com.google.protobuf.InvalidProtocolBufferException;
import org.jraft.core.FiniteStateMachine;
import org.jraft.rpc.LogEntry;
import org.jraft.kv.Command;

public final class KvStateMachine implements FiniteStateMachine {
  private static final byte[] EMPTY = new byte[0];
  private record Dedupe(long opId, boolean ok, byte[] value) {}

  private final Map<String, byte[]> store;
  private final Map<String, Dedupe> dedupeStore;

  public KvStateMachine() {
    this.store = new HashMap<>();
    this.dedupeStore = new HashMap<>();
  }

  @Override
  public ApplyResult apply(LogEntry e) {
    Command cmd = parseCommand(e);
    String client = cmd.getClientId();
    long opId = cmd.getOpId();

    // idempotence: return cached result on retry or out-of-order duplicate
    Dedupe prev = dedupeStore.get(client);
    if (prev != null && opId <= prev.opId) {
      return new ApplyResult(e.getIndex(), prev.ok, prev.value, true);
    }

    boolean ok = false;
    byte[] value = EMPTY;

    switch (cmd.getOpCase()) {
      case PUT -> {
        String key = cmd.getPut().getKey();
        byte[] data = cmd.getPut().getValue().toByteArray();
        store.put(key, data);
        ok = true;
        value = data; // intentionally echo stored value
      }
      case DEL -> {
        String key = cmd.getDel().getKey();
        store.remove(key);
        ok = true;
        // value stays EMPTY for now
      }
      case CAS -> {
        String key = cmd.getCas().getKey();
        byte[] expected = cmd.getCas().getExpected().toByteArray();
        byte[] update   = cmd.getCas().getUpdate().toByteArray();
        byte[] current  = store.get(key);

        // Semantics: missing (null) != empty bytes; only match if both null or bytes equal
        boolean matches = (current == null && expected.length == 0)
                       || (current != null && Arrays.equals(current, expected));

        if (matches) {
          store.put(key, update);
          ok = true;
          // value stays EMPTY
        } else {
          ok = false;
        }
      }
      case OP_NOT_SET -> throw new IllegalArgumentException("empty/unknown op");
    }

    dedupeStore.put(client, new Dedupe(opId, ok, value));
    return new ApplyResult(e.getIndex(), ok, value, false);
  }

  private static Command parseCommand(LogEntry e) {
    try {
      return Command.parseFrom(e.getData());
    } catch (InvalidProtocolBufferException ex) {
      throw new IllegalArgumentException("Corrupt KV command at index " + e.getIndex(), ex);
    }
  }

  public byte[] get(String key) { return store.get(key); }
  public Map<String, byte[]> snapshotView() { return Collections.unmodifiableMap(store); }
}
