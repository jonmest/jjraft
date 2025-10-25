## How to use
```java
// Client code:
var putCmd = Command.newBuilder()
    .setClientId("client-1")
    .setOpId(1)
    .setPut(Put.newBuilder().setKey("foo").setValue(ByteString.copyFromUtf8("bar")))
    .build();

long index = raftNode.propose(putCmd.toByteArray());

if (index == -1) {
  // Not leader, redirect to current leader
  String leader = raftNode.getLeaderId();
  // ... retry with leader ...
} else {
  // Wait for commit
  while (raftNode.getCommitIndex() < index) {
    Thread.sleep(10);
  }
  // Now the command is committed!
}
```