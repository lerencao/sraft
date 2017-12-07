package org.nonsense.raft

import com.google.protobuf.ByteString
import org.nonsense.raft.RaftLog.RaftResult
import org.nonsense.raft.model.RaftPB.NodeId
import org.nonsense.raft.protos.Protos._
import org.nonsense.raft.storage.Storage

sealed trait SnapshotStatus
case object Finish  extends SnapshotStatus
case object Failure extends SnapshotStatus

case class RawNode[T <: Storage](
  raft: RaftPeer[T],
  private var prevSoftState: SoftState = SoftState(),
  private var prevHardState: HardState = HardState.getDefaultInstance
) {
  def tick(): Boolean = this.raft.tick()

  // Campaign causes this RawNode to transition to candidate state.
  def campaign(): RaftResult[Unit] = {
    val msg = Message.newBuilder().setMsgType(MessageType.MsgHup).build()
    this.raft.onCampaign(msg)
  }

  def propose(data: Array[Byte], syncLog: Boolean): RaftResult[Unit] = {
    val entry = Entry
      .newBuilder()
      .setData(ByteString.copyFrom(data))
      .setSyncLog(syncLog)
      .setEntryType(EntryType.EntryNormal)
      .build()
    this.raft.onPropose(Array(entry))
  }

  def proposeConfChange(cc: ConfChange): RaftResult[Unit] = {
    val ccEntry = Entry
      .newBuilder()
      .setData(cc.toByteString)
      .setSyncLog(true)
      .setEntryType(EntryType.EntryConfChange)
      .build()
    this.raft.onPropose(Array(ccEntry))
  }

  def reportUnreachable(id: NodeId): Unit = {
    val msg = Message.newBuilder().setMsgType(MessageType.MsgUnreachable).setFrom(id).build()
    this.raft.onPeerUnreachable(msg)
  }

  def reportSnapshot(id: NodeId, status: SnapshotStatus): Unit = {
    val msg = Message
      .newBuilder()
      .setMsgType(MessageType.MsgSnapStatus)
      .setFrom(id)
      .setReject(status == Failure)
      .build()
    this.raft.onSnapStatus(msg)
  }

  def transferLeader(transferee: NodeId): Unit = {
    val msg =
      Message.newBuilder().setMsgType(MessageType.MsgTransferLeader).setFrom(transferee).build()
    this.raft.onTransferLeader(msg)
  }

  // ReadIndex requests a read state. The read state will be set in ready.
  // Read State has a read index. Once the application advances further than the read
  // index, any linearizable read requests issued before the read request can be
  // processed safely. The read state will have the same rctx attched.
  def readIndex(rctx: Array[Byte]): Unit = {
    val msg = Message
      .newBuilder()
      .setMsgType(MessageType.MsgReadIndex)
      .addEntries(Entry.newBuilder().setData(ByteString.copyFrom(rctx)))
      .build()
    this.raft.onReadIndex(msg)
  }
}

case class Peer(id: Long, context: Option[Array[Byte]])

object RawNode {

  def apply[T <: Storage](id: Long,
                          tag: String,
                          applied: Long,
                          config: Config,
                          store: T,
                          peers: Seq[Peer]): RawNode[T] = {
    assert(id != 0, "config.id must not be zero")
    val r         = RaftPeer(id = id, tag = tag, config = config, store = store, applied = applied)
    val lastIndex = r.log.lastIndex

    if (lastIndex == 0) {
      r.initWithPeers(peers)
    }

    val prevHardState = lastIndex match {
      case 0 => HardState.getDefaultInstance
      case _ => r.hardState
    }
    val rn = RawNode(raft = r, prevSoftState = r.softState, prevHardState)
    rn
  }

}
