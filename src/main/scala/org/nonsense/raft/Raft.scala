/**
  * Raft implementation
  */
package org.nonsense.raft

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.Logger
import org.nonsense.raft.RaftLog.RaftResult
import org.nonsense.raft.ReadOnlyOption.Safe
import org.nonsense.raft.model.RaftPB.{INVALID_ID, NodeId, TermT}
import org.nonsense.raft.protos.Protos
import org.nonsense.raft.protos.Protos._
import org.nonsense.raft.storage.Storage
import org.nonsense.raft.utils.Ok

import scala.collection.mutable
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
  msgs: MutableBuffer[Message] = MutableBuffer()
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

  def nodes: Seq[NodeId]  = peers.toStream.sorted
  @inline def quorum: Int = this.peers.size / 2 + 1

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
      case _: LeaderState => // just do nothing if I am leader
        logger.debug("ignoring Campaign because already leader")
      case _ =>
        val ents =
          this.log
            .slice(this.log.applied + 1, this.log.committed + 1, RaftLog.NO_LIMIT)
            .unwrap("unexpected error when getting unapplied entries")
        val n = RaftPeer.numOfPendingConf(ents)
        // TODO: need to check `committed` > `applied`
        if (n != 0) {
          logger.warn(
            s"cannot campaign at term ${this.term} " +
              s"since there are still $n pending conf changes to apply")
        } else {
          logger.info(s"starting a new election at term ${this.term}")
          this.doCampaign(RaftPeer.CAMPAIGN_ELECTION)
        }
    }
    Ok(Unit)
  }

  private def doCampaign(campaignType: String): Unit = {
    this.becomeCandidate()
    val (voteMsgType, term) = (MessageType.MsgRequestVote, this.term)

    // after become candidate(and vote for my self), check if I can become leader
    if (this.poll(this.id, voteRespMsgType(voteMsgType), approve = true) == this.quorum) {
      if (campaignType == RaftPeer.CAMPAIGN_ELECTION) {
        this.becomeLeader()
      }
    } else {
      for { peer <- this.peers if peer != this.id } {
        logger.info("")
        val m = Message
          .newBuilder()
          .setTo(peer)
          .setMsgType(voteMsgType)
          .setFrom(this.id)
          .setTerm(term)
          .setIndex(this.log.lastIndex)
          .setLogTerm(this.log.lastTerm)
        if (campaignType == RaftPeer.CAMPAIGN_TRANSFER) {
          m.setContext(ByteString.copyFromUtf8(RaftPeer.CAMPAIGN_ELECTION))
        }
        this.send(m)
      }
    }
  }

  private def send(msg: Message.Builder): Unit = {
    msg.setFrom(this.id)
    if (msg.getMsgType == MessageType.MsgRequestPreVote) {
      // if term is unset when request vote
      if (msg.getTerm == 0) {
        // Pre-vote RPCs are sent at a term other than our actual term, so the code
        // that sends these messages is responsible for setting the term.
        throw new Exception(s"$tag term should be set when sending ${msg.getMsgType}")
      }
    } else {
      if (msg.getTerm != 0) {
        throw new Exception(
          s"$tag term should not be set when sending ${msg.getMsgType}, was ${msg.getTerm}")
      } else if (msg.getMsgType == MessageType.MsgPropose || msg.getMsgType == MessageType.MsgReadIndex) {
        // do not attach term to MsgPropose, MsgReadIndex
        // proposals are a way to forward to the leader and
        // should be treated as local message.
        // MsgReadIndex is also forwarded to leader.
      } else {
        msg.setTerm(this.term)
      }
    }

    this.msgs.append(msg.build())
  }

  private def voteRespMsgType(messageType: MessageType): MessageType = {
    messageType match {
      case MessageType.MsgRequestVote => MessageType.MsgRequestVoteResponse
      case _                          => throw new Exception(s"Not a vote message: $messageType")
    }
  }
  private def poll(id: NodeId, messageType: MessageType, approve: Boolean): Int = {
    logger.info(
      s"{} receive {} {} from {} at term {}",
      this.tag,
      messageType,
      if (approve) { "approve" } else { "rejection" },
      id,
      this.term
    )

    this.states match {
      case CandidateState(votes) =>
        votes.getOrElseUpdate(id, approve)
        votes.count { case (_, x) => x }
      case _ => throw new IllegalStateException(s"$tag cannot poll in state ${this.state}")
    }
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

  def becomeCandidate(): Unit = {
    assert(this.state != Leader, s"$tag invalid transition [leader -> candidate]")
    val myTerm = this.term + 1
    this.reset(myTerm)
    val myself = this.id
    this.vote = myself
    this.states = CandidateState()
    logger.info(s"$tag become candidate at term $term")
  }

  private def becomeLeader(): Unit = {
    assert(this.state != Follower, "invalid transition [follower -> leader]")
    this.reset(this.term)
    // I am the leader
    this.leaderId = this.id

    // TODO: init progress
    val myState = LeaderState(progresses = mutable.Map())

    val begin = this.log.committed + 1
    val ents = this.log
      .entries(begin, RaftLog.NO_LIMIT)
      .unwrap("unexpected error getting uncommitted entries")
    val numOfConf = RaftPeer.numOfPendingConf(ents)
    if (numOfConf > 1) {
      throw new Exception(s"$tag unexpected double uncommitted config entry")
    }

    if (numOfConf == 1) {
      myState.pendingConf = true
    }
    this.states = myState
    this.appendEntry(mutable.Buffer(Entry.newBuilder()))
    logger.info("{} became leader at term {}", this.tag, this.term)
  }

  private def appendEntry(partialEnts: mutable.Buffer[Protos.Entry.Builder]): Unit = {
    val lastIdx = this.log.lastIndex
    val myTerm  = this.term

    val ents = for { (e, i) <- partialEnts.zipWithIndex } yield {
      e.setTerm(myTerm)
      e.setIndex(lastIdx + 1 + i)
      e.build()
    }
    this.log.append(ents)

    this.states match {
      case LeaderState(progresses, _) =>
        progresses(this.id).maybeUpdate(this.log.lastIndex)
      case _ => throw new Exception("can only append entry in leader state")
    }
    // Regardless of maybe_commit's return, our caller will call bcastAppend.
    // TODO: review our caller
    this.maybeCommit()
  }

  // maybe_commit attempts to advance the commit index. Returns true if
  // the commit index changed (in which case the caller should call
  // r.bcast_append).
  private def maybeCommit(): Boolean = {
    this.states match {
      case LeaderState(progresses, _) =>
        val mis = progresses.values
          .map(p => p.matched)
          .toBuffer
          .sortWith((a, b) => a > b)
        val mci = mis(this.quorum - 1)
        this.log.maybeCommit(mci, this.term)
      case _ => false
    }
  }

  def addNode(id: NodeId): Unit = {
    this.peers.add(id)

    // only leader role needs to add new progress
    this.states match {
      case LeaderState(prs, _) if !prs.contains(id) =>
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

  val CAMPAIGN_ELECTION = "CampaignElection"
  val CAMPAIGN_TRANSFER = "CampaignTransfer"

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

  def numOfPendingConf(entries: mutable.Buffer[Entry]): Int =
    entries.count(e => e.getEntryType == EntryType.EntryConfChange)
}
