package org.nonsense.raft.model

import org.nonsense.raft.protos.Protos.{ConfState, HardState}

case class RaftState(confState: ConfState, hardState: HardState)
