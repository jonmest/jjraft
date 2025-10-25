package org.jraft.net;

import java.util.function.Consumer;

import org.jraft.rpc.AppendEntriesRequest;
import org.jraft.rpc.AppendEntriesResponse;
import org.jraft.rpc.RequestVoteRequest;
import org.jraft.rpc.RequestVoteResponse;

public interface RaftTransport {
  public void requestVote(String peerId, RequestVoteRequest req, Consumer<RequestVoteResponse> cb);
  public void appendEntries(String peerId, AppendEntriesRequest req, Consumer<AppendEntriesResponse> cb);
  
}
