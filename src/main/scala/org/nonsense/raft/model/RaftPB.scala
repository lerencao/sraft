package org.nonsense.raft.model

object RaftPB {
  type NodeId = Long
  type TermT  = Long
  type IndexT = Long
  val INVALID_ID: Long = 0
}
