package org.nonsense.raft.model

import org.nonsense.raft.model.RaftMessage.MessageType
import org.nonsense.raft.model.RaftPB.NodeId
import org.nonsense.raft.protos.Protos.{Entry, Snapshot}

case class RaftMessage(
  msgType: MessageType = 0,
  from: NodeId = 0,
  to: NodeId = 0,
  term: Long = 0,
  logTerm: Long = 0,
  index: Long = 0,
  entries: Vector[Entry] = Vector(),
  commit: Long = 0,
  snapshot: Option[Snapshot] = None,
  reject: Boolean = false,
  reject_hint: Long = 0,
  context: Array[Byte] = Array()
)

object RaftMessage {
  type MessageType = Long
  def apply(to: NodeId, msgType: MessageType, from: Option[NodeId]): RaftMessage = {
    val msg = RaftMessage(msgType = msgType, to = to)
    from match {
      case None => msg
      case Some(f) => msg.copy(from = f)
    }
  }
}
