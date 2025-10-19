package org.jraft.kv;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.ByteString;
import org.jraft.rpc.LogEntry;

final class KvStateMachineTest {

  private static LogEntry mkEntry(long idx, long term, Command cmd) {
    return LogEntry.newBuilder()
        .setIndex(idx)
        .setTerm(term)
        .setData(cmd.toByteString())
        .build();
  }

  @Test
  void putStoresBytes_andIsIdempotentByClientOp() {
    var kv = new KvStateMachine();

    var cmd1 = Command.newBuilder()
        .setClientId("c1").setOpId(1)
        .setPut(Put.newBuilder()
            .setKey("a")
            .setValue(ByteString.copyFromUtf8("x")))
        .build();

    var e1 = mkEntry(1, 1, cmd1);
    var r1 = kv.apply(e1);
    assertTrue(r1.ok());
    assertArrayEquals("x".getBytes(), kv.get("a"));

    var r2 = kv.apply(e1);
    assertTrue(r2.dedupHit());
    assertArrayEquals("x".getBytes(), kv.get("a"));
  }

  @Test
  void casSucceedsWhenExpectedMatches_thenFailsOnStaleExpected() {
    var kv = new KvStateMachine();

    // Put k="x"
    var put = Command.newBuilder()
        .setClientId("c1").setOpId(1)
        .setPut(Put.newBuilder()
            .setKey("k")
            .setValue(ByteString.copyFromUtf8("x")))
        .build();
    kv.apply(mkEntry(1, 1, put));

    // CAS k: x -> y  (should succeed)
    var casOk = Command.newBuilder()
        .setClientId("c1").setOpId(2)
        .setCas(Cas.newBuilder()
            .setKey("k")
            .setExpected(ByteString.copyFromUtf8("x"))
            .setUpdate(ByteString.copyFromUtf8("y")))
        .build();
    var r1 = kv.apply(mkEntry(2, 1, casOk));
    assertTrue(r1.ok());
    assertArrayEquals("y".getBytes(), kv.get("k"));

    // CAS k: x -> z  (stale expected, should fail)
    var casFail = Command.newBuilder()
        .setClientId("c1").setOpId(3)
        .setCas(Cas.newBuilder()
            .setKey("k")
            .setExpected(ByteString.copyFromUtf8("x"))
            .setUpdate(ByteString.copyFromUtf8("z")))
        .build();
    var r2 = kv.apply(mkEntry(3, 1, casFail));
    assertFalse(r2.ok());
    assertArrayEquals("y".getBytes(), kv.get("k"));
  }

  @Test
  void delRemovesKey_andIsIdempotent() {
    var kv = new KvStateMachine();

    var put = Command.newBuilder()
        .setClientId("c1").setOpId(1)
        .setPut(Put.newBuilder()
            .setKey("gone")
            .setValue(ByteString.copyFromUtf8("v")))
        .build();
    kv.apply(mkEntry(1, 1, put));
    assertNotNull(kv.get("gone"));

    var del = Command.newBuilder()
        .setClientId("c1").setOpId(2)
        .setDel(Del.newBuilder().setKey("gone"))
        .build();

    var r1 = kv.apply(mkEntry(2, 1, del));
    assertTrue(r1.ok());
    assertNull(kv.get("gone"));

    // Retry same delete (idempotent)
    var r2 = kv.apply(mkEntry(2, 1, del));
    assertTrue(r2.dedupHit());
    assertNull(kv.get("gone"));
  }
}
