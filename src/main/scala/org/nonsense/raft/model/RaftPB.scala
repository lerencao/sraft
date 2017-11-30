package org.nonsense.raft.model

object RaftPB {
  type NodeId = Long

  trait EntryType
  case object EntryNormal extends EntryType
  case object EntryConfChange extends EntryType

  trait ConfChangeType
  case object AddNode extends ConfChangeType
  case object RemoveNode extends ConfChangeType
  case object AddLearnerNode extends ConfChangeType

  case class ConfChange(id: Long, changeType: ConfChangeType, nodeId: NodeId, context: Array[Byte])

  case class Entry(entryType: EntryType = null, term: Long = 0, index: Long = 0, data: Array[Byte] = Array(), syncLog: Boolean = false)

  case class ConfState(nodes: List[Long] = List(), learners: List[Long] = List())
  case class HardState(term: Long = 0, vote: NodeId = 0, commit: Long = 0)

  case class RaftState(confState: ConfState, hardState: HardState)
  case class SnapshotMetadata(confState: ConfState = ConfState(), index: Long = 0, term: Long = 0)
  case class Snapshot(data: Array[Byte] = Array(), metadata: SnapshotMetadata = SnapshotMetadata())
}
