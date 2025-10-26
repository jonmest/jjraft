package org.jraft.node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.jraft.core.FollowerHandlers;
import org.jraft.core.RepeatingTask;
import org.jraft.core.StateMachine;
import org.jraft.net.RaftTransport;
import org.jraft.rpc.AppendEntriesRequest;
import org.jraft.rpc.AppendEntriesResponse;
import org.jraft.rpc.LogEntry;
import org.jraft.rpc.NodeId;
import org.jraft.rpc.RequestVoteRequest;
import org.jraft.rpc.RequestVoteResponse;
import org.jraft.state.LogStore;
import org.jraft.state.RaftState;
import org.jraft.state.RaftState.Role;
import org.jraft.util.ElectionTimer;

import com.google.protobuf.ByteString;

public class RaftNode {
  private final String id;
  private final List<String> peers;
  
  private final RaftState raftState;
  private final StateMachine stateMachine;
  private final LogStore log;
  private final RaftTransport net;
  private final ElectionTimer electionTimer;

  private final RepeatingTask heartbeatTask;
  private static final long HEARTBEAT_PERIOD_MS = 100;

  private static final long MIN_ELECTION_MS = 250;
  private static final long MAX_ELECTION_MS = 1000;
  private final AtomicInteger electionResetProbe = new AtomicInteger();

  private int votesGranted = 0;
  public final Map<String, Long> nextIndex = new HashMap<>();
  public final Map<String, Long> matchIndex = new HashMap<>();
  


  public RaftNode(String id, List<String> peers, RaftState state, 
                  LogStore log, RaftTransport net, StateMachine stateMachine,
                  RepeatingTask heartbeatTask, ElectionTimer electionTimer) {
    this.id = id; this.peers = peers; this.raftState = state;
    this.stateMachine = stateMachine; this.log = log; this.net = net;
    this.heartbeatTask = heartbeatTask; this.electionTimer = electionTimer;

    if (this.electionTimer != null) {
      this.electionTimer.start(MIN_ELECTION_MS, MAX_ELECTION_MS, this::onElectionTick);
    }
  }

  public void onElectionTick() {
    if (raftState.getRole() == RaftState.Role.LEADER) return;
    startElection();
  }

  public int getElectionResetCount() { return electionResetProbe.get(); }

  public void resetElectionTimer() {
    if (this.electionTimer != null) {
      this.electionTimer.reset();
      electionResetProbe.incrementAndGet();
    }
  }

  public void startElection() {
    raftState.setCurrentTerm(raftState.getCurrentTerm() + 1);
    raftState.setVotedFor(id);
    raftState.becomeCandidate();
    votesGranted = 1;

    var lastIndex = log.lastIndex(); var lastTerm = log.termAt(lastIndex);

    var request =  RequestVoteRequest.newBuilder()
                    .setTerm(raftState.getCurrentTerm())
                    .setCandidateId(NodeId.newBuilder().setId(id))
                    .setLastLogIndex(lastIndex)
                    .setLastLogTerm(lastTerm)
                    .build();

    for (String peer : peers) {
      net.requestVote(peer, request, resp -> onRequestVoteResponse(peer, resp));
    }
  }

  private int majority() { return (peers.size() + 1) / 2 + 1;}

  private void stepDown(long newTerm) {
    if (heartbeatTask != null) heartbeatTask.stop();
    
    raftState.setCurrentTerm(newTerm);
    raftState.setVotedFor(null);
    raftState.becomeFollower();
    raftState.setLeader(null);

    if (electionTimer != null && !electionTimer.isRunning()) {
      electionTimer.start(MIN_ELECTION_MS, MAX_ELECTION_MS, this::onElectionTick);
    } else if (electionTimer != null) {
      electionTimer.reset();
    }
  }


  private void onRequestVoteResponse(String peerId, RequestVoteResponse resp) {
    if (resp.getTerm() > raftState.getCurrentTerm()) {
      stepDown(resp.getTerm());
      return;
    }

    var role = raftState.getRole();

    if (role != Role.CANDIDATE) {
      System.out.println("Not a candidate");
      return;
    }
    if (!resp.getVoteGranted()) return;

    votesGranted++;
    if (votesGranted >= majority()) {
      // Election is won, become leader!
      becomeLeader();
    }
  }


  private void becomeLeader() {
    raftState.becomeLeader();
    raftState.setLeader(id);

    if (electionTimer != null) {
      this.electionTimer.stop();
    }

    applyCommitedEntries();

    long ni = log.lastIndex() + 1;
    nextIndex.clear(); matchIndex.clear();
    for (String p : peers) {
      nextIndex.put(p, ni);
      matchIndex.put(p, 0L);
    }

    // Immediately send one round of heartbeats
    sendHeartbeats();

    if (heartbeatTask != null && !heartbeatTask.isRunning()) {
      heartbeatTask.start(this::sendHeartbeats, HEARTBEAT_PERIOD_MS);
    }
  }

  private void sendAppendEntriesToPeer(String p) {
    long next = nextIndex.get(p);
    long prev = next - 1;
    long prevTerm = log.termAt(prev);

    List<LogEntry> entries = new ArrayList<>();
    long lastIdx = log.lastIndex();
    for (long i = next; i <= lastIdx; i++) {
      entries.add(log.entryAt(i));
    }

    var req = AppendEntriesRequest.newBuilder()
        .setTerm(raftState.getCurrentTerm())
        .setLeaderId(NodeId.newBuilder().setId(id))
        .setPrevLogIndex(prev)
        .setPrevLogTerm(prevTerm)
        .setLeaderCommit(raftState.getCommitIndex())
        .addAllEntries(entries)
        .build();

    net.appendEntries(p, req, (resp) -> onAppendEntriesResponse(p, prev, entries.size(), resp));
  }

  public void sendHeartbeats() {
    if (raftState.getRole() != RaftState.Role.LEADER) return;
    for (String p : peers) sendAppendEntriesToPeer(p);
  }

  private void advanceCommitIndex() {
    for (long n = log.lastIndex(); n > raftState.getCommitIndex(); n--) {
      // only commit entries from the current term
      if (log.termAt(n) != raftState.getCurrentTerm()) continue;
      
      // how many nodes have this entry?
      int replicationCount = 1; // leader has it!
      for (String peer : peers) {
        if (matchIndex.getOrDefault(peer, 0L) >= n) {
          replicationCount++;
        }
      }

      if (replicationCount >= majority()) {
        raftState.setCommitIndex(n);
        applyCommitedEntries();
        return;
      }
    }
  }

  private void applyCommitedEntries() {
    while (raftState.getLastApplied() < raftState.getCommitIndex()) {
      long index = raftState.getLastApplied() + 1;
      var entry = log.entryAt(index);
      if (stateMachine != null) {
        stateMachine.apply(entry);
      }
      raftState.setLastApplied(index);
    }
  }


  private void onAppendEntriesResponse(String peerId, long sentPrevIndex, int entriesCount, AppendEntriesResponse resp) {
    if (resp.getTerm() > raftState.getCurrentTerm()) {
      stepDown(resp.getTerm());
      return;
    }
    if (raftState.getRole() != RaftState.Role.LEADER) return;

    if (resp.getSuccess()) {
      long match = sentPrevIndex + entriesCount;
      matchIndex.put(peerId, match);
      nextIndex.put(peerId, match + 1);
      advanceCommitIndex();
      return;
    }

    long currentNext = nextIndex.getOrDefault(peerId, 1L);
    if (currentNext > 1) {
      long backedOff = currentNext - 1;   // move left by one
      nextIndex.put(peerId, backedOff);
      // immediate retry using the new (prevIndex, prevTerm)
      sendAppendEntriesToPeer(peerId);
    } else {
      // nextIndex already at 1 → prevIdx will be 0; retry once more at base
      // (this is idempotent; it may have already been tried)
      sendAppendEntriesToPeer(peerId);
    }
  }


  public AppendEntriesResponse onAppendEntriesRequest(AppendEntriesRequest req) {
    // Reset election timer if term is >= current (even on failure)
    // This prevents unnecessary elections when receiving valid heartbeats
    if (req.getTerm() >= raftState.getCurrentTerm()) {
      resetElectionTimer();
    }
    return FollowerHandlers.onAppendEntries(stateMachine, log, raftState, req);
  }

  public RequestVoteResponse onRequestVoteRequest(RequestVoteRequest req) {
    var response = FollowerHandlers.onRequestVote(log, raftState, req);
    if (response.getVoteGranted()) {
      resetElectionTimer();
    }
    return response;
  }

  public long propose(byte[] data) {
    if (raftState.getRole() != Role.LEADER) return -1;

    long index = log.lastIndex() + 1;
    long term = raftState.getCurrentTerm();

    var entry = LogEntry.newBuilder()
                  .setIndex(index)
                  .setTerm(term)
                  .setData(ByteString.copyFrom(data))
                  .build();
    
    log.append(List.of(entry));
    sendHeartbeats();
    return index;
  }

  // --- accessors for testing ---

  public RaftState getRaftState() { return raftState; }
  public LogStore getLog() { return log; }
  public StateMachine getStateMachine() { return stateMachine; }

}
