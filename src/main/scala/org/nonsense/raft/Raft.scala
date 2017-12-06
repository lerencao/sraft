/**
  * Raft implementation
  */
package org.nonsense.raft

import com.typesafe.scalalogging.Logger
import org.nonsense.raft.ReadOnlyOption.Safe
import org.nonsense.raft.model.RaftMessage
import org.nonsense.raft.model.RaftPB.NodeId
import org.nonsense.raft.protos.Protos._
import org.nonsense.raft.storage.Storage

import scala.collection.mutable.{Buffer => MutableBuffer}
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
  id: Long = 0,
  tag: String = "",
  peers: Seq[NodeId] = Seq(),
  applied: Long = 0,
  electionTimeout: Int = 0,
  heartbeatTimeout: Int = 0,
  maxSizePerMsg: Long = 0,
  maxInflightMegs: Long = 0,
  checkQuorum: Boolean = false,
  preVote: Boolean = false,
  readOnlyOptions: ReadOnlyOption = Safe,
  skipBcastCommit: Boolean = false
)

//sealed trait RaftRole[T <: Storage]
case class SoftState(leaderId: Long = RaftPeer.INVALID_ID, stateRole: RaftRole = Follower)

case class RaftPeer[T <: Storage] private (
  log: RaftLog[T],
  config: Config,
  tag: String,
  id: NodeId,
  var state: RaftRole = Follower,
  var peers: Seq[NodeId],
  var term: Long,
  var vote: NodeId,
  var leaderId: NodeId = 0,
  var electionElapsed: Int = 0,
  var heartbeatElapsed: Int = 0,
  var randomizedElectionTimeout: Int = 0,
  msgs: MutableBuffer[RaftMessage] = MutableBuffer()
) {
  def addNode(id: NodeId): Unit = ???

  def hardState: HardState =
    HardState.newBuilder.setTerm(this.term).setVote(this.vote).setCommit(this.log.committed).build()
  def softState: SoftState = SoftState(leaderId = this.leaderId, stateRole = this.state)

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
  }

  private def reset(term: Long): Unit = {
    if (this.term != term) {
      this.term = term
      this.vote = RaftPeer.INVALID_ID
    }
    this.leaderId = RaftPeer.INVALID_ID
    this.resetRandomizedElectionTimeout()
    this.electionElapsed = 0
    this.heartbeatElapsed = 0
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
  val logger             = Logger(this.getClass)
  val INVALID_ID: NodeId = 0

  def apply[T <: Storage](store: T,
                          peers: Seq[NodeId],
                          applied: Long,
                          config: Config): RaftPeer[T] = {
    import scala.collection.JavaConverters._

    val raftState = store.initialState().left.get
    if (raftState.confState.getNodesCount > 0 && peers.nonEmpty) {
      throw new IllegalArgumentException(
        s"cannot specify both new($peers) and ConfState.Nodes(${raftState.confState.getNodesList})")
    }
    val prs: Seq[NodeId] = if (peers.nonEmpty) {
      peers
    } else {
      raftState.confState.getNodesList.asScala.map(l => l.toLong)
    }

    val raftLog = RaftLog(store, applied, config.tag)

    val follower = RaftPeer(
      log = raftLog,
      config = config,
      id = config.id,
      tag = config.tag,
      peers = prs,
      term = raftState.hardState.getTerm,
      vote = raftState.hardState.getVote
    )

    follower.becomeFollower(raftState.hardState.getTerm, INVALID_ID)
    logger.info(
      "{} newRaft [peers: {}, term: {}, commit: {}, applied: {}, last_index: {}, last_term: {}]",
      follower.tag,
      follower.peers,
      follower.term,
      follower.log.committed,
      follower.log.applied,
      follower.log.lastIndex,
      follower.log.lastTerm
    )
    follower
  }
}
