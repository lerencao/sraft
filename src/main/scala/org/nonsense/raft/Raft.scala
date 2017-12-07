/**
  * Raft implementation
  */
package org.nonsense.raft

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.Logger
import org.nonsense.raft.RaftLog.RaftResult
import org.nonsense.raft.ReadOnlyOption.Safe
import org.nonsense.raft.model.RaftMessage
import org.nonsense.raft.model.RaftPB.{INVALID_ID, NodeId, TermT}
import org.nonsense.raft.protos.Protos._
import org.nonsense.raft.storage.Storage
import org.nonsense.raft.utils.Ok

import scala.collection.mutable.{Buffer => MutableBuffer, Set => MutableSet}
import scala.concurrent.forkjoin.ThreadLocalRandom

sealed trait ReadOnlyOption

object ReadOnlyOption {
  object Safe       extends ReadOnlyOption
  object LeaseBased extends ReadOnlyOption
}

sealed trait RaftInternalMessage
object ElectionTimeoutMsg extends RaftInternalMessage
object VoteSelfMsg        extends RaftInternalMessage

case class Config(
//  id: Long = 0,
//  tag: String = "",
//  peers: Seq[NodeId] = Seq(),
//  applied: Long = 0,
  electionTimeout: Int = 0,
  heartbeatTimeout: Int = 0,
  maxSizePerMsg: Long = 0,
  maxInflightMegs: Int = 0,
  checkQuorum: Boolean = false,
  preVote: Boolean = false,
  readOnlyOptions: ReadOnlyOption = Safe,
  skipBcastCommit: Boolean = false
)

//sealed trait RaftRole[T <: Storage]
case class SoftState(leaderId: Long = INVALID_ID, stateRole: RaftRole = Follower)

case class RaftPeer[T <: Storage] private (
  log: RaftLog[T],
  config: Config,
  tag: String,
  id: NodeId,
  var peers: MutableSet[NodeId],
  var term: TermT,
  var vote: NodeId,
  var leaderId: NodeId = 0,
  var electionElapsed: Int = 0,
  var heartbeatElapsed: Int = 0,
  var randomizedElectionTimeout: Int = 0,
  var states: RoleState = FollowerState(),
  msgs: MutableBuffer[RaftMessage] = MutableBuffer()
) {
  def onReadIndex(msg: Message): Unit = ???

  def onTransferLeader(msg: Message): Unit = ???

  def onSnapStatus(msg: Message): Unit = ???

  def onPeerUnreachable(msg: Message): Unit = ???

  def onPropose(entries: Array[Entry]): RaftResult[Unit] = ???

  val logger = Logger(this.getClass)

  def initWithPeers(peers: Seq[Peer]): Unit = {
    assert(this.log.lastIndex == 0)
    val initialTerm  = 1
    val initialIndex = 1
    this.becomeFollower(initialTerm, INVALID_ID)
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
    this.log.append(entries)
    this.log.committed = entries.length
    for (peer <- peers) {
      this.addNode(peer.id)
    }
  }

  def nodes: Seq[NodeId] = peers.toStream.sorted

  def hardState: HardState =
    HardState.newBuilder.setTerm(this.term).setVote(this.vote).setCommit(this.log.committed).build()

  def softState: SoftState = SoftState(leaderId = this.leaderId, stateRole = this.state)

  def state: RaftRole = this.states match {
    case _: FollowerState  => Follower
    case _: LeaderState    => Leader
    case _: CandidateState => Candidate
  }

  def tick(): Boolean = ???

  def onCampaign(msg: Message): RaftResult[Unit] = {
    this.states match {
      // just do nothing if I am leader
      case _: LeaderState =>
      case _              =>
//        this.log.slice(this.log.applied + 1, this.log.committed + 1)
    }

    Ok(Unit)
  }

  private def resetRandomizedElectionTimeout(): Unit = {
    val prevTimeout = this.randomizedElectionTimeout
    val timeout = ThreadLocalRandom
      .current()
      .nextInt(this.config.electionTimeout, 2 * this.config.electionTimeout)
    this.randomizedElectionTimeout = timeout
  }

  def becomeFollower(term: Long, leaderId: NodeId): Unit = {
    this.reset(term)
    this.leaderId = leaderId
    this.states = FollowerState()
    logger.info(s"$tag become follower at term($term), leader_id($leaderId)")
  }

  def addNode(id: NodeId): Unit = {
    this.peers.add(id)

    // only leader role needs to add new progress
    this.states match {
      case LeaderState(prs) if !prs.contains(id) =>
        // TODO: handle leader state change
        val lastIdx = this.log.lastIndex
        val p       = Progress(0L, lastIdx + 1, this.config.maxInflightMegs)
        prs(id) = p
    }
  }

  private def reset(term: Long): Unit = {
    if (this.term != term) {
      this.term = term
      this.vote = INVALID_ID
    }
    this.leaderId = INVALID_ID
    this.electionElapsed = 0
    this.heartbeatElapsed = 0
    this.resetRandomizedElectionTimeout()
  }

//  def becomeCandidate() = ???
//  def tick(): RaftPeer[T] = {
//    this.electionElapsed += 1
//
//    if (this.isElectionTimeouted) {
//      this.electionElapsed = 0
//      this.state = Candidate
//      this.vote = this.id
//    }
//  }

  private def isElectionTimeouted = this.electionElapsed >= this.randomizedElectionTimeout
}

object RaftPeer {
  val logger = Logger(this.getClass)

  def apply[T <: Storage](store: T,
                          id: NodeId,
                          tag: String,
//                          peers: Seq[NodeId],
                          applied: Long,
                          config: Config): RaftPeer[T] = {
    import scala.collection.JavaConverters._

    val raftLog = RaftLog(store, applied, tag)

    val rs = store.initialState().get

    // NOTICE: Always init peers from log, even no peers exist
    val existedPeers: Seq[NodeId] = rs.confState.getNodesList.asScala.map(l => l.toLong)

    val follower = RaftPeer(
      log = raftLog,
      config = config,
      id = id,
      tag = tag,
      peers = MutableSet(existedPeers: _*),
      term = rs.hardState.getTerm,
      vote = rs.hardState.getVote
    )

    follower.becomeFollower(rs.hardState.getTerm, INVALID_ID)
    logger.info(
      "{} newRaft [peers: {}, term: {}, commit: {}, applied: {}, last_index: {}, last_term: {}]",
      follower.tag,
      follower.nodes,
      follower.term,
      follower.log.committed,
      follower.log.applied,
      follower.log.lastIndex,
      follower.log.lastTerm
    )
    follower
  }
}
