package org.jraft.viz;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.jraft.kv.Command;
import org.jraft.kv.Put;
import org.jraft.kv.Del;
import org.jraft.kv.KvStateMachine;
import org.jraft.node.RaftNode;

/**
 * simple http server that serves the raft visualization UI.
 *
 * serves:
 *   - static html/css/js from resources
 *   - /api/cluster endpoint with real-time cluster state as JSON
 *
 * usage:
 *   RaftHttpServer server = new RaftHttpServer(8080, visualizer);
 *   server.start();
 *   // open http://localhost:8080 in browser
 */
public class RaftHttpServer {

  private final HttpServer server;
  private final RaftVisualizer visualizer;
  private final Object transport;  // VisualizerDemo.InMemoryTransport
  private final Gson gson;
  private final ScheduledExecutorService scheduler;

  public RaftHttpServer(int port, RaftVisualizer visualizer, Object transport) throws IOException {
    this.visualizer = visualizer;
    this.transport = transport;
    this.gson = new GsonBuilder().setPrettyPrinting().create();
    this.scheduler = Executors.newScheduledThreadPool(1, r -> {
      Thread t = new Thread(r, "auto-recovery-scheduler");
      t.setDaemon(true);
      return t;
    });
    this.server = HttpServer.create(new InetSocketAddress(port), 0);

    // serve static resources
    server.createContext("/", this::handleStaticFile);

    // API endpoints
    server.createContext("/api/cluster", this::handleClusterState);
    server.createContext("/api/kv", this::handleKvState);
    server.createContext("/api/commands", this::handleCommands);
    server.createContext("/api/nodes", this::handleNodeControl);

    server.setExecutor(null); // use default executor
  }

  public void start() {
    server.start();
    System.out.println("🌐 HTTP server started on http://localhost:" + server.getAddress().getPort());
    System.out.println("   Open this URL in your browser to see the visualization\n");
  }

  public void stop() {
    server.stop(0);
  }

  /**
   * serve the static index.html from resources
   */
  private void handleStaticFile(HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();

    // default to index.html
    if (path.equals("/")) {
      path = "/index.html";
    }

    // try to load from resources
    try {
      String resourcePath = "static" + path;
      byte[] content = getClass().getClassLoader()
        .getResourceAsStream(resourcePath)
        .readAllBytes();

      // set content type based on file extension
      String contentType = getContentType(path);
      exchange.getResponseHeaders().set("Content-Type", contentType);

      exchange.sendResponseHeaders(200, content.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(content);
      }
    } catch (Exception e) {
      // file not found
      String response = "404 Not Found";
      exchange.sendResponseHeaders(404, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  /**
   * serve current cluster state as JSON
   */
  private void handleClusterState(HttpExchange exchange) throws IOException {
    if (!exchange.getRequestMethod().equals("GET")) {
      exchange.sendResponseHeaders(405, -1);
      return;
    }

    // build cluster state JSON
    ClusterStateResponse state = buildClusterState();
    String json = gson.toJson(state);

    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");

    byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  /**
   * serve key-value store state as JSON
   */
  private void handleKvState(HttpExchange exchange) throws IOException {
    if (!exchange.getRequestMethod().equals("GET")) {
      exchange.sendResponseHeaders(405, -1);
      return;
    }

    // collect KV state from all nodes
    KvStateResponse response = new KvStateResponse();
    response.stores = new java.util.ArrayList<>();

    // find the highest term among all nodes to identify the real leader
    long maxTerm = visualizer.getNodes().values().stream()
      .mapToLong(n -> n.getRaftState().getCurrentTerm())
      .max()
      .orElse(0);

    for (Map.Entry<String, RaftNode> entry : visualizer.getNodes().entrySet()) {
      String nodeId = entry.getKey();
      RaftNode node = entry.getValue();

      // get the state machine if it's a KvStateMachine
      if (node.getStateMachine() instanceof KvStateMachine) {
        KvStateMachine kv = (KvStateMachine) node.getStateMachine();

        NodeKvState nodeState = new NodeKvState();
        nodeState.nodeId = nodeId;
        // only mark as leader if it's a leader at the highest term (not a stale leader)
        nodeState.isLeader = node.getRaftState().getRole() == org.jraft.state.RaftState.Role.LEADER
                           && node.getRaftState().getCurrentTerm() == maxTerm;
        nodeState.entries = new java.util.ArrayList<>();

        // convert byte[] values to base64 strings for JSON
        for (Map.Entry<String, byte[]> kvEntry : kv.snapshotView().entrySet()) {
          KvEntry e = new KvEntry();
          e.key = kvEntry.getKey();
          e.value = new String(kvEntry.getValue(), StandardCharsets.UTF_8);
          nodeState.entries.add(e);
        }

        response.stores.add(nodeState);
      }
    }

    String json = gson.toJson(response);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");

    byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  /**
   * handle command submissions (POST) or list commands (GET)
   */
  private void handleCommands(HttpExchange exchange) throws IOException {
    if (exchange.getRequestMethod().equals("POST")) {
      handleCommandSubmission(exchange);
    } else {
      exchange.sendResponseHeaders(405, -1);
    }
  }

  /**
   * submit a new command to the leader
   */
  private void handleCommandSubmission(HttpExchange exchange) throws IOException {
    // read request body
    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    CommandRequest request = gson.fromJson(body, CommandRequest.class);

    // find the leader with the highest term (handles split brain situations)
    RaftNode leader = visualizer.getNodes().values().stream()
      .filter(n -> n.getRaftState().getRole() == org.jraft.state.RaftState.Role.LEADER)
      .max((a, b) -> Long.compare(a.getRaftState().getCurrentTerm(), b.getRaftState().getCurrentTerm()))
      .orElse(null);

    CommandResponse response = new CommandResponse();

    if (leader == null) {
      response.success = false;
      response.error = "no leader elected";
    } else {
      try {
        // build the command based on operation type
        Command.Builder cmdBuilder = Command.newBuilder()
          .setClientId(request.clientId != null ? request.clientId : "web-ui")
          .setOpId(System.currentTimeMillis());

        switch (request.operation.toLowerCase()) {
          case "put" -> {
            cmdBuilder.setPut(Put.newBuilder()
              .setKey(request.key)
              .setValue(ByteString.copyFrom(request.value.getBytes(StandardCharsets.UTF_8)))
              .build());
          }
          case "del", "delete" -> {
            cmdBuilder.setDel(Del.newBuilder()
              .setKey(request.key)
              .build());
          }
          default -> {
            response.success = false;
            response.error = "unknown operation: " + request.operation;
            sendJsonResponse(exchange, response);
            return;
          }
        }

        // propose the command
        byte[] cmdBytes = cmdBuilder.build().toByteArray();
        long index = leader.propose(cmdBytes);

        response.success = true;
        response.index = index;
        response.message = "command proposed at index " + index;
      } catch (Exception e) {
        response.success = false;
        response.error = e.getMessage();
      }
    }

    sendJsonResponse(exchange, response);
  }

  /**
   * handle node control operations (kill/revive)
   */
  private void handleNodeControl(HttpExchange exchange) throws IOException {
    if (!exchange.getRequestMethod().equals("POST")) {
      exchange.sendResponseHeaders(405, -1);
      return;
    }

    // read request body
    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    NodeControlRequest request = gson.fromJson(body, NodeControlRequest.class);

    NodeControlResponse response = new NodeControlResponse();

    if ("kill-leader".equals(request.action)) {
      // find the leader with highest term (handles split brain situations)
      var leaderEntry = visualizer.getNodes().entrySet().stream()
        .filter(e -> e.getValue().getRaftState().getRole() == org.jraft.state.RaftState.Role.LEADER)
        .max((a, b) -> Long.compare(a.getValue().getRaftState().getCurrentTerm(), b.getValue().getRaftState().getCurrentTerm()));

      if (leaderEntry.isPresent()) {
        String leaderId = leaderEntry.get().getKey();

        // partition the node in the transport (simulate network failure)
        try {
          transport.getClass().getMethod("partitionNode", String.class).invoke(transport, leaderId);
        } catch (Exception e) {
          System.err.println("Failed to partition node: " + e.getMessage());
        }

        // schedule automatic recovery after 10 seconds
        scheduler.schedule(() -> {
          try {
            System.out.println("🔄 Auto-recovering node " + leaderId + " after 10 seconds of partition");
            transport.getClass().getMethod("unpartitionNode", String.class).invoke(transport, leaderId);
          } catch (Exception e) {
            System.err.println("Failed to unpartition node: " + e.getMessage());
          }
        }, 10, TimeUnit.SECONDS);

        // note: we keep the node in the visualizer so it shows as offline/partitioned
        // this way the cluster topology remains correct for quorum calculations

        response.success = true;
        response.message = "Leader " + leaderId + " killed (network partitioned, will auto-recover in 10s)";
        response.killedNode = leaderId;
      } else {
        response.success = false;
        response.error = "No leader found";
      }
    } else {
      response.success = false;
      response.error = "Unknown action: " + request.action;
    }

    sendJsonResponse(exchange, response);
  }

  /**
   * helper to send JSON response
   */
  private void sendJsonResponse(HttpExchange exchange, Object obj) throws IOException {
    String json = gson.toJson(obj);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");

    byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  /**
   * build the cluster state response from visualizer
   */
  private ClusterStateResponse buildClusterState() {
    ClusterStateResponse response = new ClusterStateResponse();

    // find current term and leader
    response.term = visualizer.getNodes().values().stream()
      .mapToLong(n -> n.getRaftState().getCurrentTerm())
      .max()
      .orElse(0);

    response.leader = visualizer.getNodes().entrySet().stream()
      .filter(e -> e.getValue().getRaftState().getRole() == org.jraft.state.RaftState.Role.LEADER)
      .map(Map.Entry::getKey)
      .findFirst()
      .orElse(null);

    // build node info
    response.nodes = visualizer.getNodes().entrySet().stream()
      .map(entry -> {
        String id = entry.getKey();
        var node = entry.getValue();
        var state = node.getRaftState();

        NodeInfo info = new NodeInfo();
        info.id = id;
        info.role = state.getRole().name();
        info.term = state.getCurrentTerm();
        info.votedFor = state.getVotedFor();
        info.leader = state.getLeader();

        // log info
        info.log = new LogInfo();
        info.log.lastIndex = node.getLog().lastIndex();
        info.log.commitIndex = state.getCommitIndex();
        info.log.lastApplied = state.getLastApplied();

        // replication info (for leaders)
        if (state.getRole() == org.jraft.state.RaftState.Role.LEADER) {
          info.replication = new java.util.HashMap<>();
          for (Map.Entry<String, Long> e : node.matchIndex.entrySet()) {
            String peer = e.getKey();
            long matchIdx = e.getValue();
            double progress = info.log.lastIndex > 0
              ? (matchIdx * 1.0 / info.log.lastIndex)
              : 1.0;

            ReplicationInfo repInfo = new ReplicationInfo();
            repInfo.matchIndex = matchIdx;
            repInfo.nextIndex = node.nextIndex.get(peer);
            repInfo.progress = progress;

            info.replication.put(peer, repInfo);
          }
        }

        return info;
      })
      .toList();

    return response;
  }

  private String getContentType(String path) {
    if (path.endsWith(".html")) return "text/html";
    if (path.endsWith(".css")) return "text/css";
    if (path.endsWith(".js")) return "application/javascript";
    if (path.endsWith(".json")) return "application/json";
    return "text/plain";
  }

  // JSON response classes
  private static class ClusterStateResponse {
    long term;
    String leader;
    java.util.List<NodeInfo> nodes;
  }

  private static class NodeInfo {
    String id;
    String role;
    long term;
    String votedFor;
    String leader;
    LogInfo log;
    Map<String, ReplicationInfo> replication;
  }

  private static class LogInfo {
    long lastIndex;
    long commitIndex;
    long lastApplied;
  }

  private static class ReplicationInfo {
    long matchIndex;
    long nextIndex;
    double progress;
  }

  // KV API response classes
  private static class KvStateResponse {
    java.util.List<NodeKvState> stores;
  }

  private static class NodeKvState {
    String nodeId;
    boolean isLeader;
    java.util.List<KvEntry> entries;
  }

  private static class KvEntry {
    String key;
    String value;
  }

  // Command API request/response classes
  private static class CommandRequest {
    String operation;  // "put" or "del"
    String key;
    String value;      // optional, only for put
    String clientId;   // optional
  }

  private static class CommandResponse {
    boolean success;
    long index;
    String message;
    String error;
  }

  // Node control API request/response classes
  private static class NodeControlRequest {
    String action;  // "kill-leader"
  }

  private static class NodeControlResponse {
    boolean success;
    String message;
    String error;
    String killedNode;
  }
}
