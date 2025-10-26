package org.jraft.viz;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.jraft.node.RaftNode;
import org.jraft.state.RaftState;

/**
 * real-time visualization of raft cluster state.
 *
 * displays:
 *   - node roles (follower, candidate, leader)
 *   - current terms
 *   - election timeout countdowns
 *   - log indices (lastIndex, commitIndex, lastApplied)
 *   - leader/follower relationships
 *
 * usage:
 *   RaftVisualizer viz = new RaftVisualizer();
 *   viz.addNode("node-1", node1);
 *   viz.addNode("node-2", node2);
 *   viz.addNode("node-3", node3);
 *   viz.printClusterState();  // print to console
 */
public class RaftVisualizer {

  private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();

  public void addNode(String id, RaftNode node) {
    nodes.put(id, node);
  }

  public Map<String, RaftNode> getNodes() {
    return nodes;
  }

  public void removeNode(String id) {
    nodes.remove(id);
  }

  /**
   * print current cluster state to console with nice formatting
   */
  public void printClusterState() {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("RAFT CLUSTER STATE");
    System.out.println("=".repeat(80));

    // find current term and leader
    long maxTerm = nodes.values().stream()
      .mapToLong(n -> n.getRaftState().getCurrentTerm())
      .max()
      .orElse(0);

    String leaderId = nodes.values().stream()
      .filter(n -> n.getRaftState().getRole() == RaftState.Role.LEADER)
      .map(n -> nodes.entrySet().stream()
        .filter(e -> e.getValue() == n)
        .map(Map.Entry::getKey)
        .findFirst()
        .orElse("unknown"))
      .findFirst()
      .orElse("(no leader)");

    System.out.printf("Current Term: %d | Leader: %s\n", maxTerm, leaderId);
    System.out.println("-".repeat(80));

    // sort nodes by id for consistent display
    List<Map.Entry<String, RaftNode>> sortedNodes = nodes.entrySet().stream()
      .sorted(Map.Entry.comparingByKey())
      .collect(Collectors.toList());

    for (Map.Entry<String, RaftNode> entry : sortedNodes) {
      String id = entry.getKey();
      RaftNode node = entry.getValue();
      printNodeState(id, node);
    }

    System.out.println("=".repeat(80) + "\n");
  }

  /**
   * print detailed state for a single node
   */
  private void printNodeState(String id, RaftNode node) {
    RaftState state = node.getRaftState();
    String role = formatRole(state.getRole());
    String roleSymbol = getRoleSymbol(state.getRole());

    System.out.printf("\n%s %s %s\n", roleSymbol, id, roleSymbol);
    System.out.println("  " + "-".repeat(40));

    // basic state
    System.out.printf("  Role: %s\n", role);
    System.out.printf("  Term: %d\n", state.getCurrentTerm());

    if (state.getVotedFor() != null) {
      System.out.printf("  Voted For: %s\n", state.getVotedFor());
    }

    if (state.getLeader() != null) {
      System.out.printf("  Leader: %s\n", state.getLeader());
    }

    // log state
    long lastIndex = node.getLog().lastIndex();
    long commitIndex = state.getCommitIndex();
    long lastApplied = state.getLastApplied();

    System.out.printf("  Log: lastIndex=%d, commitIndex=%d, lastApplied=%d\n",
      lastIndex, commitIndex, lastApplied);

    // show log status bar
    printLogStatusBar(lastIndex, commitIndex, lastApplied);

    // leader-specific info
    if (state.getRole() == RaftState.Role.LEADER) {
      System.out.println("  Replication:");
      for (Map.Entry<String, Long> entry : node.nextIndex.entrySet()) {
        String peer = entry.getKey();
        long nextIdx = entry.getValue();
        long matchIdx = node.matchIndex.getOrDefault(peer, 0L);
        double progress = lastIndex > 0 ? (matchIdx * 100.0 / lastIndex) : 100.0;
        System.out.printf("    %s: matchIndex=%d, nextIndex=%d [%s%% synced]\n",
          peer, matchIdx, nextIdx, String.format("%.0f", progress));
      }
    }
  }

  /**
   * visual progress bar for log state
   */
  private void printLogStatusBar(long lastIndex, long commitIndex, long lastApplied) {
    if (lastIndex == 0) {
      System.out.println("  [empty log]");
      return;
    }

    int barWidth = 40;
    int appliedWidth = (int) ((lastApplied * barWidth) / lastIndex);
    int committedWidth = (int) ((commitIndex * barWidth) / lastIndex);

    System.out.print("  [");
    for (int i = 0; i < barWidth; i++) {
      if (i < appliedWidth) {
        System.out.print("█");  // applied
      } else if (i < committedWidth) {
        System.out.print("▓");  // committed but not applied
      } else {
        System.out.print("░");  // uncommitted
      }
    }
    System.out.println("]");
    System.out.println("   █=applied ▓=committed ░=uncommitted");
  }

  private String formatRole(RaftState.Role role) {
    return switch (role) {
      case FOLLOWER -> "FOLLOWER";
      case CANDIDATE -> "CANDIDATE";
      case LEADER -> "LEADER";
    };
  }

  private String getRoleSymbol(RaftState.Role role) {
    return switch (role) {
      case FOLLOWER -> "👥";
      case CANDIDATE -> "🗳️";
      case LEADER -> "👑";
    };
  }

  /**
   * generate ascii art diagram of cluster topology
   */
  public void printClusterTopology() {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("CLUSTER TOPOLOGY");
    System.out.println("=".repeat(80));

    // find leader
    String leaderId = nodes.entrySet().stream()
      .filter(e -> e.getValue().getRaftState().getRole() == RaftState.Role.LEADER)
      .map(Map.Entry::getKey)
      .findFirst()
      .orElse(null);

    if (leaderId != null) {
      System.out.println("\n                    👑 LEADER");
      System.out.printf("                   [%s]\n", leaderId);
      System.out.println("                      |");
      System.out.println("        ┌─────────────┼─────────────┐");
      System.out.println("        |             |             |");
      System.out.println("        ↓             ↓             ↓");

      // show followers
      List<String> followers = nodes.entrySet().stream()
        .filter(e -> e.getValue().getRaftState().getRole() == RaftState.Role.FOLLOWER)
        .map(Map.Entry::getKey)
        .sorted()
        .collect(Collectors.toList());

      if (followers.isEmpty()) {
        System.out.println("              (no followers)");
      } else {
        for (int i = 0; i < followers.size(); i++) {
          String spacing = " ".repeat(10 + i * 16);
          System.out.printf("%s👥 [%s]\n", spacing, followers.get(i));
        }
      }
    } else {
      // no leader - show all nodes as candidates/followers
      System.out.println("\n  (no leader elected yet)");
      System.out.println();

      for (Map.Entry<String, RaftNode> entry : nodes.entrySet()) {
        String id = entry.getKey();
        RaftState.Role role = entry.getValue().getRaftState().getRole();
        String symbol = getRoleSymbol(role);
        System.out.printf("  %s [%s] - %s\n", symbol, id, formatRole(role));
      }
    }

    System.out.println("\n" + "=".repeat(80) + "\n");
  }

  /**
   * compact one-line summary of cluster state
   */
  public String getClusterSummary() {
    long term = nodes.values().stream()
      .mapToLong(n -> n.getRaftState().getCurrentTerm())
      .max()
      .orElse(0);

    long leaders = nodes.values().stream()
      .filter(n -> n.getRaftState().getRole() == RaftState.Role.LEADER)
      .count();

    long candidates = nodes.values().stream()
      .filter(n -> n.getRaftState().getRole() == RaftState.Role.CANDIDATE)
      .count();

    long followers = nodes.values().stream()
      .filter(n -> n.getRaftState().getRole() == RaftState.Role.FOLLOWER)
      .count();

    return String.format("Term %d: %d leader, %d candidates, %d followers (%d nodes total)",
      term, leaders, candidates, followers, nodes.size());
  }

  /**
   * start background thread that prints cluster state periodically
   */
  public Thread startMonitoring(long intervalMs) {
    Thread monitor = new Thread(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(intervalMs);
          printClusterState();
        } catch (InterruptedException e) {
          break;
        }
      }
    });
    monitor.setDaemon(true);
    monitor.start();
    return monitor;
  }
}
