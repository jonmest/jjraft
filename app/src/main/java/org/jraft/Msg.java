package org.jraft;

import java.util.concurrent.CompletableFuture;

import org.jraft.rpc.AppendEntriesRequest;
import org.jraft.rpc.AppendEntriesResponse;
import org.jraft.rpc.RequestVoteRequest;
import org.jraft.rpc.RequestVoteResponse;

sealed interface Msg permits TickElection, TickHeartbeat, RpcAppendReq, RpcVoteReq, ClientPropose, RpcReplyComplete {}

record TickElection() implements Msg {}
record TickHeartbeat() implements Msg {}

// gRPC -> actor; include a place to complete replies
record RpcAppendReq(AppendEntriesRequest req, CompletableFuture<AppendEntriesResponse> reply) implements Msg {}
record RpcVoteReq(RequestVoteRequest req, CompletableFuture<RequestVoteResponse> reply) implements Msg {}

// client proposal (e.g., a command)
record ClientPropose(byte[] data, CompletableFuture<Boolean> ack) implements Msg {}

// replication completion notifications (from per-peer sender)
record RpcReplyComplete(String followerId, long sentLastIndex, AppendEntriesResponse resp) implements Msg {}
