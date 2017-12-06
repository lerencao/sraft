package org.nonsense.raft

import com.google.protobuf.ByteString
import org.nonsense.raft.model.RaftPB.INVALID_ID
import org.nonsense.raft.protos.Protos._
import org.nonsense.raft.storage.Storage

case class RawNode[T <: Storage](
  raft: RaftPeer[T],
  private var prevSoftState: SoftState = SoftState(),
  private var prevHardState: HardState = HardState.getDefaultInstance
)

case class Peer(id: Long, context: Option[Array[Byte]])

object RawNode {

  def apply[T <: Storage](id: Long,
                          tag: String,
                          applied: Long,
                          config: Config,
                          store: T,
                          peers: Seq[Peer]): RawNode[T] = {
    assert(id != 0, "config.id must not be zero")
    val r = RaftPeer(id = id,
                     tag = tag,
                     config = config,
                     store = store,
                     peers = peers.map(p => p.id),
                     applied = applied)
    val lastIndex = r.log.lastIndex

    if (lastIndex == 0) {
      val initialTerm  = 1
      val initialIndex = 1
      r.becomeFollower(initialTerm, INVALID_ID)
      val entries: Seq[Entry] = peers.zipWithIndex.map {
        case (p, idx) =>
          val cc = ConfChange.newBuilder().setChangeType(ConfChangeType.AddNode).setNodeId(p.id)
          if (p.context.nonEmpty) {
            cc.setContext(ByteString.copyFrom(p.context.get))
          }

          Entry
            .newBuilder()
            .setData(cc.build().toByteString)
            .setEntryType(EntryType.EntryConfChange)
            .setTerm(initialTerm)
            .setIndex(idx + initialIndex)
            .build()
      }
      r.log.append(entries)
      r.log.committed = entries.length
      for (peer <- peers) {
        r.addNode(peer.id)
      }
    }

    val prevHardState = lastIndex match {
      case 0 => HardState.getDefaultInstance
      case _ => r.hardState
    }
    val rn = RawNode(raft = r, prevSoftState = r.softState, prevHardState)
    rn
  }
}
