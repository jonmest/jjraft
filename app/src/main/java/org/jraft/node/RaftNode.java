package org.jraft.node;

import org.jraft.core.FiniteStateMachine;
import org.jraft.state.RaftState;

final class RaftNode {
  RaftState raftState;
  FiniteStateMachine fsm;
}
