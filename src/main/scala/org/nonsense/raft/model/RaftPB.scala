package org.nonsense.raft.model

import com.google.protobuf.Message
import org.nonsense.raft.RaftLog
import org.nonsense.raft.protos.Protos.Entry

import scala.collection.mutable

object RaftPB {
  type NodeId = Long
  type TermT  = Long
  type IndexT = Long
  val INVALID_ID: Long = 0

  def limitSize(ents: mutable.Buffer[Entry], maxSize: Long): Unit = {
    val l = getLimitAtSize(ents, maxSize)
    ents.trimEnd(ents.size - l)
  }

  private def getLimitAtSize[T <: Message, I <% Iterable[T]](entris: I, max: Long): Int = {
    if (max == RaftLog.NO_LIMIT) {
      entris.size
    } else {
      val it = entris.iterator
      if (!it.hasNext) {
        return 0
      }

      // if the first entry size is bigger than max, still return it
      var size  = it.next().getSerializedSize
      var limit = 1

      while (it.hasNext && size <= max) {
        size += it.next().getSerializedSize
        if (size <= max) {
          limit += 1
        }
      }

      limit
    }
  }
}
