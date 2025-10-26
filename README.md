# jjraft

A from-scratch implementation of the Raft consensus algorithm in Java, including a distributed key-value store with full persistence and crash recovery.

## Overview

This project implements the core Raft consensus protocol as described in the [Raft paper](https://raft.github.io/raft.pdf). The implementation includes:

- **Leader election** with randomized timeouts
- **Log replication** across cluster nodes
- **Safety guarantees** ensuring consistency
- **Persistent storage** using write-ahead logging
- **Crash recovery** that restores full state
- **Key-value store** with PUT, DELETE, and CAS operations
- **Client operation idempotence** via deduplication

## Status

The core Raft algorithm is fully implemented and tested. The system can:
- Elect leaders and maintain consensus
- Replicate operations across nodes
- Survive crashes and recover state
- Handle network partitions and failures

Not yet implemented:
- Log compaction via snapshotting
- Cluster membership changes
- gRPC transport layer (interface exists)

## Quick Start

### Prerequisites

- Java 25
- Gradle 9.0+

### Building

```bash
./gradlew build
./gradlew test    # run test suite (97 tests)
```

### Running the Visualization

```bash
./gradlew run
```

This starts a local web server at `http://localhost:8080` where you can see the cluster state in real-time.

## Architecture

```
org.jraft/
├── core/           - Core Raft algorithm
│   ├── RaftAlgorithms.java
│   ├── FollowerHandlers.java
│   └── StateMachine.java
├── node/           - Node coordination
│   ├── RaftNode.java
│   └── RaftNodeFactory.java
├── state/          - State management
│   ├── RaftState.java
│   ├── FileLogStore.java
│   └── PersistentState.java
├── kv/             - Key-value store
│   └── KvStateMachine.java
└── net/            - Network abstraction
    └── RaftTransport.java
```

## Persistence

The system uses a write-ahead log (WAL) for durability:

```
/data/node-1/
├── metadata.json          # term, votedFor
└── log/
    ├── wal-0000001.log   # log segments
    └── wal-0000002.log
```

All writes are fsync'd to disk. Crash recovery rebuilds in-memory state from disk, handling partial writes gracefully.

**Key properties:**
- Term and vote changes are atomic (temp file + rename)
- Log entries use protobuf serialization
- Segment rotation at 10MB (configurable)
- Index rebuilt on startup

## Usage

### Creating a Cluster

```java
// Create nodes with persistent storage
RaftNode node1 = RaftNodeFactory.create(
  "node-1",
  List.of("node-2", "node-3"),
  Paths.get("/data/node-1"),
  new KvStateMachine(),
  transport
);

RaftNode node2 = RaftNodeFactory.create("node-2", ...);
RaftNode node3 = RaftNodeFactory.create("node-3", ...);
```

### Proposing Operations

```java
// Build command
Command cmd = Command.newBuilder()
    .setClientId("client-1")
    .setOpId(1)
    .setPut(Put.newBuilder()
        .setKey("foo")
        .setValue(ByteString.copyFromUtf8("bar")))
    .build();

// Propose to leader
long index = node.propose(cmd.toByteArray());

if (index == -1) {
  // Not leader - redirect to node.getRaftState().getLeader()
} else {
  // Wait for commit
  while (node.getRaftState().getCommitIndex() < index) {
    Thread.sleep(10);
  }
  // Operation is now durable and replicated
}
```

## Testing

The test suite covers:

- **Algorithm correctness** - log matching, safety properties
- **Persistence** - crash recovery, partial writes
- **Integration** - end-to-end flows
- **Edge cases** - network failures, leader changes

```bash
./gradlew test --tests "org.jraft.state.*"      # persistence tests
./gradlew test --tests "org.jraft.node.*"       # integration tests
./gradlew test --tests "org.jraft.core.*"       # algorithm tests
```

## Implementation Notes

### Timing Parameters

- Election timeout: 250-500ms (randomized)
- Heartbeat interval: 100ms
- Timeout ratio: 5:1 (recommended by paper)

These values work well for local networks. Production systems may need tuning based on network latency.

### Idempotence

The KV state machine deduplicates operations:

```java
// Client retries are safe
client.put("key", "value", opId=1);  // succeeds
client.put("key", "value", opId=1);  // returns cached result
```

Each client operation includes `(clientId, opId)`. The state machine tracks the last executed operation per client.

### File-Based Storage

The implementation uses a custom file-based WAL rather than an embedded database like RocksDB. This was a deliberate choice to:
- Understand log-structured storage mechanics
- Control fsync semantics precisely
- Keep dependencies minimal
- Learn the fundamentals

For production use, RocksDB would offer better performance.

## Design Decisions

**Why protobuf for serialization?**
Version-safe, compact, and already used for RPC messages.

**Why not implement InstallSnapshot yet?**
The core algorithm works without it. Snapshotting adds complexity and is better done after the fundamentals are solid.

**Why synchronized methods in FileLogStore?**
Simplicity. The log is not a bottleneck in this implementation. A production system would use more sophisticated concurrency control.

## Performance

Current implementation prioritizes correctness over performance:
- Synchronous writes with fsync
- Single-threaded log appends
- No batching or pipelining

Measured on localhost:
- ~1000 writes/second (3-node cluster)
- ~10ms commit latency (median)

These numbers are acceptable for learning/demo purposes. Production optimizations would include write batching, pipelining, and asynchronous replication.

## References

- [Raft Paper](https://raft.github.io/raft.pdf) - Original specification
- [Raft Visualization](https://raft.github.io/) - Interactive demo
- [etcd Raft](https://github.com/etcd-io/raft) - Production implementation (Go)

## Development

This project was built to deeply understand distributed consensus. Key learnings:
- State machine replication requires determinism everywhere
- Persistence is harder than it looks (fsync, atomicity, corruption)
- Testing distributed systems needs careful thought about failure modes
- The Raft paper is well-written but details matter

## License

MIT
