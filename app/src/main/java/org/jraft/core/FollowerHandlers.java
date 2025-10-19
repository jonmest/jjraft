package org.jraft.core;

import java.util.Objects;

import org.jraft.rpc.AppendEntriesRequest;
import org.jraft.rpc.AppendEntriesResponse;
import org.jraft.rpc.RequestVoteRequest;
import org.jraft.rpc.RequestVoteResponse;
import org.jraft.state.LogStore;
import org.jraft.state.RaftState;

final class FollowerHandlers {
  public static AppendEntriesResponse onAppendEntries(FiniteStateMachine fsm, LogStore log, RaftState state, AppendEntriesRequest req) {
    // Never accept commands or votes from older terms
    if (state.getCurrentTerm() > req.getTerm()) {
      return AppendEntriesResponse.newBuilder()
        .setTerm(state.getCurrentTerm())
        .setSuccess(false)
        .build();
    }

    // Update our own state if the request is on a newer term
    if (state.getCurrentTerm() < req.getTerm()) {
      state.setCurrentTerm(req.getTerm());
      // New term, new vote! Also downgrade to follower if leader since we're outdated.
      state.clearVote();
      state.becomeFollower();
    }

    state.setLeader(req.getLeaderId().getId());

    var res = RaftAlgorithms.applyLogPatch(log, req.getPrevLogIndex(), req.getPrevLogTerm(), req.getEntriesList());
  
    if (!res.accepted()) {
      return AppendEntriesResponse.newBuilder()
        .setTerm(state.getCurrentTerm())
        .setSuccess(false)
        .build();
    }

    long newCommit = Math.min(req.getLeaderCommit(), log.lastIndex());
    if (newCommit > state.getCommitIndex()) {
      state.setCommitIndex(Math.max(state.getCommitIndex(), newCommit));
      while (state.getLastApplied() < state.getCommitIndex()) {
        long index = state.getLastApplied() + 1;
        var entry = log.entryAt(index);
        fsm.apply(entry);
        state.setLastApplied(index);
      }
    }
    return AppendEntriesResponse.newBuilder()
      .setTerm(state.getCurrentTerm())
      .setSuccess(true)
      .setMatchIndex(Math.max(req.getPrevLogIndex(), res.lastNewIndex()))
      .build();
  }

  public static RequestVoteResponse onRequestVote(LogStore log, RaftState state, RequestVoteRequest req) {
    // Never accept commands or votes from older terms
    if (req.getTerm() < state.getCurrentTerm()) {
      return RequestVoteResponse.newBuilder()
        .setTerm(state.getCurrentTerm())
        .setVoteGranted(false)
        .build();
    }

    if (req.getTerm() > state.getCurrentTerm()) {
      state.setCurrentTerm(req.getTerm());
      state.clearVote();
      state.becomeFollower();
    }

    var notVotedYet = state.getVotedFor() == null;
    var sameCandidate = Objects.equals(state.getVotedFor(), req.getCandidateId().getId());
    var canVote = notVotedYet || sameCandidate;
    var upToDate = RaftAlgorithms.isCandidateUpToDate(log, req.getLastLogTerm(), req.getLastLogIndex());
    var grant = canVote && upToDate;

    if (grant) {
      state.setVotedFor(req.getCandidateId().getId());
      // todo: when timer is added later: resetElectionTimer();
    }

    return RequestVoteResponse.newBuilder()
      .setTerm(state.getCurrentTerm())
      .setVoteGranted(grant)
      .build();
  }
}
