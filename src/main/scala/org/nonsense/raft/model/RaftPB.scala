package org.nonsense.raft.model

import com.google.protobuf.Message
import org.nonsense.raft.RaftLog
import org.nonsense.raft.protos.Protos.{Entry, MessageType}

import scala.collection.mutable

object RaftPB {
  type NodeId = Long
  type TermT  = Long
  type IndexT = Long
  val INVALID_ID: Long = 0

  def isLocalMessage(m: MessageType): Boolean = m match {
    case MessageType.MsgHup | MessageType.MsgBeat | MessageType.MsgUnreachable |
        MessageType.MsgSnapStatus | MessageType.MsgCheckQuorum =>
      true
    case _ => false
  }

  def isResponseMessage(m: MessageType): Boolean = m match {
    case MessageType.MsgAppendResponse | MessageType.MsgRequestVoteResponse |
        MessageType.MsgHeartbeatResponse | MessageType.MsgUnreachable |
        MessageType.MsgRequestPreVoteResponse =>
      true
    case _ => false
  }

  def limitSize(ents: mutable.Buffer[Entry], maxSize: Long): Unit = {
    val l = getLimitAtSize(ents.iterator, maxSize)
    ents.trimEnd(ents.size - l)
  }

  private def getLimitAtSize[T <: Message](iter: Iterator[T], max: Long): Int = {
    if (max == RaftLog.NO_LIMIT) {
      iter.length
    } else {
      if (!iter.hasNext) {
        return 0
      }

      // if the first entry size is bigger than max, still return it
      var size  = iter.next().getSerializedSize
      var limit = 1

      while (iter.hasNext && size <= max) {
        size += iter.next().getSerializedSize
        if (size <= max) {
          limit += 1
        }
      }

      limit
    }
  }
}
