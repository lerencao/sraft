/**
  * Raft implementation
  */
package org.nonsense.raft

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.Logger
import org.nonsense.raft.RaftLog.RaftResult
import org.nonsense.raft.RaftPeer.{CAMPAIGN_ELECTION, CAMPAIGN_PRE_ELECTION}
import org.nonsense.raft.ReadOnlyOption.Safe
import org.nonsense.raft.model.RaftPB
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
  private var role: RaftRole = Follower,
  // follower have no special state

  // candidate state
  votes: mutable.Map[NodeId, Boolean] = mutable.Map(),
  // leader state
  prs: mutable.Map[NodeId, Progress] = mutable.Map(),
  private var hasPendingConf: Boolean = false,
  // message send by me
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

  def softState: SoftState = SoftState(leaderId = this.leaderId, stateRole = this.role)

  def tick(): Boolean = ???

  def onRaftMessage(msg: Message): Unit = {
    val msgType = msg.getMsgType
    // TODO: replace this with msg type
    assert(msg.getTerm != 0, "only handle msg from outside")

    // handle lower term message first
    if (msg.getTerm < this.term) {
      // We have received messages from a leader at a lower term. It is possible
      // that these messages were simply delayed in the network, but this could
      // also mean that this node has advanced its term number during a network
      // partition, and it is now unable to either win an election or to rejoin
      // the majority on the old term. If checkQuorum is false, this will be
      // handled by incrementing term numbers in response to MsgVote with a higher
      // term, but if checkQuorum is true we may not advance the term on MsgVote and
      // must generate other messages to advance the term. The net result of these
      // two features is to minimize the disruption caused by nodes that have been
      // removed from the cluster's configuration: a removed node will send MsgVotes
      // which will be ignored, but it will not receive MsgApp or MsgHeartbeat, so it
      // will not create disruptive term increases
      if (this.config.checkQuorum && (msgType == MessageType.MsgHeartbeat || msgType == MessageType.MsgAppend)) {
        // TODO: refer to raft paper to find this optimization
        //        val to_send: Nothing = new_message(m.get_from, MessageType.MsgAppendResponse, None)
      } else {
        this.logIgnoreLowerTermMessage(msg)
      }

      return
    }

    // then check `Raft-4.2.3`
    if (msg.getTerm > this.term) {
      msgType match {
        case MessageType.MsgRequestPreVote | MessageType.MsgRequestVote =>
          val campaignType = msg.getContext.toStringUtf8
          val force        = campaignType == RaftPeer.CAMPAIGN_TRANSFER
          val inLease = this.config.checkQuorum &&
            (this.leaderId != INVALID_ID && this.electionElapsed < this.config.electionTimeout)

          // if a server receives ReqeustVote request within the minimum election
          // timeout of hearing from a current leader, it does not update its term
          // or grant its vote
          if (!force && inLease) {
            logger.info(
              "{} [logterm: {}, index: {}, vote: {}] " +
                "ignored {:?} vote from {} [term: {}, logterm: {}, index: {}] " +
                "at term {}: " +
                "lease is not expired(remaining ticks: {})",
              this.tag,
              this.log.lastTerm,
              this.log.lastIndex,
              this.vote,
              campaignType,
              msg.getFrom,
              term,
              msg.getLogTerm,
              msg.getIndex,
              this.term,
              this.config.electionTimeout - this.electionElapsed
            )

            return
          }
      }
    }

    if (msg.getTerm > this.term) {
      this.logHigherTermMessage(msg)
      msg.getMsgType match {
        // For a pre-vote request:
        // Never change our term in response to a pre-vote request.
        //
        // For a pre-vote response with pre-vote granted:
        // We send pre-vote requests with a term in our future. If the
        // pre-vote is granted, we will increment our term when we get a
        // quorum. If it is not, the term comes from the node that
        // rejected our vote so we should become a follower at the new
        // term.
        case MessageType.MsgRequestPreVote                           =>
        case MessageType.MsgRequestPreVoteResponse if !msg.getReject =>
        case MessageType.MsgAppend | MessageType.MsgHeartbeat | MessageType.MsgSnapshot =>
          this.becomeFollower(msg.getTerm, msg.getFrom)
        case _ => this.becomeFollower(msg.getTerm, INVALID_ID)
      }
    }

    msgType match {
      case MessageType.MsgRequestPreVote | MessageType.MsgRequestVote =>
        this.onMsgRequestPreVoteOrVote(msg)

      case MessageType.MsgRequestPreVoteResponse if this.role == PreCandidate =>
        // Only handle vote responses corresponding to our candidacy (while in
        // state Candidate, we may get stale MsgPreVoteResp messages in this term from
        // our pre-candidate state).
        this.onMsgRequestPreVoteOrVoteResponse(msg)
      case MessageType.MsgRequestVoteResponse if this.role == Candidate =>
        this.onMsgRequestPreVoteOrVoteResponse(msg)
    }
  }

  // ========================================================================= MsgRequestPreVoteOrVoteResponse
  private def onMsgRequestPreVoteOrVoteResponse(message: Protos.Message): RaftResult[Unit] = {
    assert(this.role == PreCandidate || this.role == Candidate)
    assert(
      message.getMsgType == MessageType.MsgRequestPreVoteResponse ||
        message.getMsgType == MessageType.MsgRequestVoteResponse)

    val curTerm = this.term
    this.role match {
      case PreCandidate if message.getMsgType == MessageType.MsgRequestPreVoteResponse =>
        val gr = this.poll(message.getFrom, message.getMsgType, !message.getReject)
        if (this.quorum == gr) {
          this.doCampaign(CAMPAIGN_ELECTION)
        } else if (this.quorum == votes.size - gr) {
          this.becomeFollower(curTerm, INVALID_ID)
        }
        Ok(Unit)
      case Candidate if message.getMsgType == MessageType.MsgRequestVoteResponse =>
        val gr = this.poll(message.getFrom, message.getMsgType, !message.getReject)
        if (this.quorum == gr) {
          this.becomeLeader()
          this.bcastAppend()
        } else if (this.quorum == votes.size - gr) {
          this.becomeFollower(curTerm, INVALID_ID)
        }
        Ok(Unit)
      case _ => Ok(Unit)
    }
  }

  // ========================================================================= MsgHup

  /**
    * send hup(internal message) to make raft do campagin action
    * @param msg hup message
    * @return
    */
  def onMsgHup(msg: Message): RaftResult[Unit] = {
    assert(msg.getMsgType == MessageType.MsgHup)
    assert(!msg.hasTerm, "term should not be set in MsgHup")

    this.role match {
      case Leader =>
        // just do nothing if I am leader
        logger.debug("ignoring Campaign because already leader")
      case _ =>
        // TODO: check pending conf that not applied
//        val ents = this.log
//          .slice(this.log.applied + 1, this.log.committed + 1, RaftLog.NO_LIMIT)
//          .unwrap("unexpected error when getting unapplied entries")
//        val n = RaftPeer.numOfPendingConf(ents)
//        // TODO: need to check `committed` > `applied`
//        if (n != 0) {
//          logger.warn(
//            s"cannot campaign at term ${this.term} " +
//              s"since there are still $n pending conf changes to apply")
//          return Ok(Unit)
//        }

        logger.info("{} is starting a new election at term", this.tag, this.term)
        if (this.config.preVote) {
          this.doCampaign(CAMPAIGN_PRE_ELECTION)
        } else {
          this.doCampaign(CAMPAIGN_ELECTION)
        }
    }
    Ok(Unit)
  }

  private def doCampaign(campaignType: String): Unit = {
    if (campaignType == CAMPAIGN_PRE_ELECTION) {
      this.becomePreCandidate()
    } else {
      this.becomeCandidate()
    }

    val (voteMsgType, term) = campaignType match {
      case CAMPAIGN_PRE_ELECTION =>
        // Pre-vote RPCs are sent for next term before we've not incremented self.term.
        (MessageType.MsgRequestPreVote, this.term + 1)
      case _ =>
        (MessageType.MsgRequestVote, this.term)
    }

    // after become candidate(and vote for my self), check if I can become leader
    if (this.poll(this.id, voteRespMsgType(voteMsgType), approve = true) == this.quorum) {
      // We won the election after voting for ourselves (which must mean that
      // this is a single-node cluster). Advance to the next state.
      campaignType match {
        case CAMPAIGN_PRE_ELECTION =>
          this.doCampaign(CAMPAIGN_ELECTION)
        case CAMPAIGN_ELECTION =>
          this.becomeLeader()
      }
    } else {
      for { peer <- this.peers if peer != this.id } {
        logger.info("{} [logterm: {}, index: {}] sent {} request to {} at term {}",
                    this.tag,
                    this.log.lastTerm,
                    this.log.lastIndex,
                    voteMsgType,
                    peer,
                    this.term)
        val m = Message
          .newBuilder()
          .setTo(peer)
          .setMsgType(voteMsgType)
          .setFrom(this.id)
          .setTerm(term)
          .setIndex(this.log.lastIndex)
          .setLogTerm(this.log.lastTerm)
        if (campaignType == RaftPeer.CAMPAIGN_TRANSFER) {
          m.setContext(ByteString.copyFromUtf8(campaignType))
        }
        this.send(m)
      }
    }
  }

  private def becomePreCandidate(): Unit = {
    assert(this.role != Leader, "invalid transition [leader -> pre-candidate]")
    this.role = PreCandidate
    logger.info("{} became pre-candidate at term {}", this.tag, this.term)
  }

  def becomeCandidate(): Unit = {
    assert(this.role != Leader, s"$tag invalid transition [leader -> candidate]")
    val myTerm = this.term + 1
    this.reset(myTerm)
    val myself = this.id
    this.vote = myself
    this.role = Candidate
    // TODO: check candidate state
    logger.info(s"$tag become candidate at term $term")
  }

  // ========================================================================= MsgRequestPreVote && MsgRequestVote

  def onMsgRequestPreVoteOrVote(message: Protos.Message): RaftResult[Unit] = {
    assert(
      message.getMsgType == MessageType.MsgRequestPreVote || message.getMsgType == MessageType.MsgRequestVote)
    assert(message.hasTerm && message.getTerm != 0,
           "term should be set and greater than 0 in MsgRequestPreVote or MsgRequestVote")

    val resp = Message
      .newBuilder()
      .setFrom(this.id)
      .setTo(message.getFrom)
      .setMsgType(voteRespMsgType(message.getMsgType))

    // The m.get_term() > self.term clause is for MsgRequestPreVote. For MsgRequestVote
    // m.get_term() should always equal self.term
    if (message.getTerm > this.term ||
        this.vote == INVALID_ID ||
        (this.vote == message.getFrom && this.log.isUpToDate(message.getIndex, message.getTerm))) {
      resp.setReject(false)
      // Only record real votes.
      if (message.getMsgType == MessageType.MsgRequestVote) {
        this.electionElapsed = 0
        this.vote = message.getFrom
      }
    } else {
      resp.setReject(true)
    }

    this.send(resp)
    Ok(Unit)
  }

  // =========================================================================

  private def logIgnoreLowerTermMessage(m: Message): Unit = {
    logger.info(
      "{} [term: {}] ignored a {} message with lower term from {} [term: {}]",
      this.tag,
      this.term,
      m.getMsgType,
      m.getFrom,
      m.getTerm
    )
  }
  private def logHigherTermMessage(m: Message): Unit = {
    logger.info(
      "{} [term: {}] received a {} message with higher term from {} [term: {}]",
      this.tag,
      this.term,
      m.getMsgType,
      m.getFrom,
      m.getTerm
    )
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
      case MessageType.MsgRequestVote    => MessageType.MsgRequestVoteResponse
      case MessageType.MsgRequestPreVote => MessageType.MsgRequestPreVoteResponse
      case _                             => throw new Exception(s"Not a vote message: $messageType")
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

    this.role match {
      case Candidate =>
        votes.getOrElseUpdate(id, approve)
        votes.count { case (_, x) => x }
      case PreCandidate =>
        votes.getOrElseUpdate(id, approve)
        votes.count { case (_, x) => x }
      case _ => throw new IllegalStateException(s"$tag cannot poll in state ${this.role}")
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
    this.role = Follower
    logger.info(s"$tag become follower at term($term), leader_id($leaderId)")
  }

  private def becomeLeader(): Unit = {
    assert(this.role != Follower, "invalid transition [follower -> leader]")
    this.reset(this.term)
    // I am the leader
    this.leaderId = this.id

    // TODO: init progress

    val begin = this.log.committed + 1
    val ents = this.log
      .entries(begin, RaftLog.NO_LIMIT)
      .unwrap("unexpected error getting uncommitted entries")
    val numOfConf = RaftPeer.numOfPendingConf(ents)
    if (numOfConf > 1) {
      throw new Exception(s"$tag unexpected double uncommitted config entry")
    }

    if (numOfConf == 1) {
      this.hasPendingConf = true
    }

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

    this.role match {
      case Leader =>
        prs(this.id).maybeUpdate(this.log.lastIndex)
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
    this.role match {
      case Leader =>
        val mis = prs.values
          .map(p => p.matched)
          .toBuffer
          .sortWith((a, b) => a > b)
        val mci = mis(this.quorum - 1)
        this.log.maybeCommit(mci, this.term)
      case _ => false
    }
  }

  private def bcastAppend(): Unit = {
    for {
      p <- this.peers if p != this.id
    } {
      this.sendAppend(p)
    }
  }

  // send_append sends RPC, with entries to the given peer.
  private def sendAppend(id: RaftPB.NodeId): Unit = {}

  def addNode(id: NodeId): Unit = {
    this.peers.add(id)

    // only leader role needs to add new progress
    this.role match {
      case Leader if !prs.contains(id) =>
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

  private def isElectionTimeouted = this.electionElapsed >= this.randomizedElectionTimeout
}

object RaftPeer {
  val logger = Logger(this.getClass)

  val CAMPAIGN_PRE_ELECTION = "CampaignPreElection"
  val CAMPAIGN_ELECTION     = "CampaignElection"
  val CAMPAIGN_TRANSFER     = "CampaignTransfer"

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

//  def isSentFrom(role: RaftRole, msg: Message): Boolean = {
//    role match {
//      case Follower  => false
//      case Candidate => ???
//    }
//  }

  def numOfPendingConf(entries: mutable.Buffer[Entry]): Int =
    entries.count(e => e.getEntryType == EntryType.EntryConfChange)
}
