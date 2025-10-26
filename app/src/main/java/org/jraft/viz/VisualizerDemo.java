package org.jraft.viz;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.jraft.kv.Command;
import org.jraft.kv.KvStateMachine;
import org.jraft.kv.Put;
import org.jraft.net.RaftTransport;
import org.jraft.node.RaftNode;
import org.jraft.node.RaftNodeFactory;
import org.jraft.rpc.AppendEntriesRequest;
import org.jraft.rpc.AppendEntriesResponse;
import org.jraft.rpc.RequestVoteRequest;
import org.jraft.rpc.RequestVoteResponse;

import com.google.protobuf.ByteString;

/**
 * demo program showing raft visualization in action.
 *
 * creates a 3-node cluster, runs an election, and shows
 * the cluster state with the visualizer.
 *
 * run with: ./gradlew run
 */
public class VisualizerDemo {

  public static void main(String[] args) throws Exception {
    System.out.println("starting raft visualization demo...\n");
    Path dataDir = Files.createDirectories(Path.of(System.getProperty("user.home"), "jraft-demo"));

    Path dataDir1 = dataDir.resolve("node-1");
    Path dataDir2 = dataDir.resolve("node-2");
    Path dataDir3 = dataDir.resolve("node-3");

    InMemoryTransport transport = new InMemoryTransport();

    System.out.println("creating 3-node cluster...");
    RaftNode node1 = RaftNodeFactory.create(
      "node-1",
      List.of("node-2", "node-3"),
      dataDir1,
      new KvStateMachine(),
      transport.getTransportFor("node-1")
    );

    RaftNode node2 = RaftNodeFactory.create(
      "node-2",
      List.of("node-1", "node-3"),
      dataDir2,
      new KvStateMachine(),
      transport.getTransportFor("node-2")
    );

    RaftNode node3 = RaftNodeFactory.create(
      "node-3",
      List.of("node-1", "node-2"),
      dataDir3,
      new KvStateMachine(),
      transport.getTransportFor("node-3")
    );

    transport.registerNode("node-1", node1);
    transport.registerNode("node-2", node2);
    transport.registerNode("node-3", node3);

    RaftVisualizer viz = new RaftVisualizer();
    viz.addNode("node-1", node1);
    viz.addNode("node-2", node2);
    viz.addNode("node-3", node3);

    System.out.println("cluster created!\n");

    RaftHttpServer httpServer = new RaftHttpServer(8080, viz, transport);
    httpServer.start();

    System.out.println("INITIAL STATE:");
    viz.printClusterState();
    viz.printClusterTopology();

    Thread.sleep(1000);

    System.out.println("\n⚡ node-1 starting election...\n");
    node1.startElection();
    Thread.sleep(100);  // let RPCs propagate

    viz.printClusterState();
    viz.printClusterTopology();

    Thread.sleep(1000);

    // if (node1.getRaftState().getRole() == org.jraft.state.RaftState.Role.LEADER) {
    //   System.out.println("\n📝 proposing commands to leader...\n");

    //   for (int i = 1; i <= 5; i++) {
    //     Command cmd = Command.newBuilder()
    //       .setClientId("demo-client")
    //       .setOpId(i)
    //       .setPut(Put.newBuilder()
    //         .setKey("key" + i)
    //         .setValue(ByteString.copyFromUtf8("value" + i)))
    //       .build();

    //     long index = node1.propose(cmd.toByteArray());
    //     System.out.printf("  proposed command %d → index %d\n", i, index);
    //     Thread.sleep(50);
    //   }

    //   Thread.sleep(200);
    //   viz.printClusterState();

    //   System.out.println("\n📨 waiting for replication and commits...\n");
    //   Thread.sleep(500);

    //   viz.printClusterState();
    // }

    System.out.println("\n✅ demo complete!\n");
    System.out.println("cluster summary: " + viz.getClusterSummary());

    System.out.println("\n🌐 web visualization is running!");
    System.out.println("   press ctrl+C to stop and cleanup\n");

    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      System.out.println("\nshutting down...");
    }

    httpServer.stop();
    System.out.println("cleaning up temp files...");
    deleteDirectory(dataDir);
    System.out.println("done!");
  }

  /**
   * in-memory transport that connects nodes directly without network
   */
  static class InMemoryTransport {
    private final Map<String, RaftNode> nodes = new HashMap<>();
    private final java.util.Set<String> partitionedNodes = new java.util.HashSet<>();

    public void registerNode(String id, RaftNode node) {
      nodes.put(id, node);
    }

    public synchronized void partitionNode(String nodeId) {
      partitionedNodes.add(nodeId);
    }

    public synchronized void unpartitionNode(String nodeId) {
      partitionedNodes.remove(nodeId);
    }

    private synchronized boolean isPartitioned(String nodeId) {
      return partitionedNodes.contains(nodeId);
    }

    public RaftTransport getTransportFor(String sourceId) {
      return new RaftTransport() {
        @Override
        public void requestVote(String peerId, RequestVoteRequest req, Consumer<RequestVoteResponse> callback) {
          new Thread(() -> {
            try {
              // check partition status BEFORE sleep to prevent race condition
              if (isPartitioned(sourceId) || isPartitioned(peerId)) {
                // message dropped - network partition
                return;
              }
              Thread.sleep(500);
              // check again AFTER sleep in case partition happened during sleep
              if (isPartitioned(sourceId) || isPartitioned(peerId)) {
                // message dropped - network partition
                return;
              }
              RaftNode peer = nodes.get(peerId);
              if (peer != null) {
                RequestVoteResponse response = peer.onRequestVoteRequest(req);
                callback.accept(response);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }).start();
        }

        @Override
        public void appendEntries(String peerId, AppendEntriesRequest req, Consumer<AppendEntriesResponse> callback) {
          // simulate async network call
          new Thread(() -> {
            try {
              // check partition status BEFORE sleep to prevent race condition
              if (isPartitioned(sourceId) || isPartitioned(peerId)) {
                // message dropped - network partition
                return;
              }
              Thread.sleep(500);  // 500ms network delay
              // check again AFTER sleep in case partition happened during sleep
              if (isPartitioned(sourceId) || isPartitioned(peerId)) {
                // message dropped - network partition
                return;
              }
              RaftNode peer = nodes.get(peerId);
              if (peer != null) {
                AppendEntriesResponse response = peer.onAppendEntriesRequest(req);
                callback.accept(response);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }).start();
        }
      };
    }
  }

  private static void deleteDirectory(Path path) throws IOException {
    if (Files.exists(path)) {
      Files.walk(path)
        .sorted((a, b) -> -a.compareTo(b))  // delete children first
        .forEach(p -> {
          try { Files.deleteIfExists(p); } catch (IOException e) {}
        });
    }
  }
}
