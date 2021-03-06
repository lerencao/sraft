/**
  * Raft implementation
  */
package org.nonsense.raft

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.Logger
import org.nonsense.raft.RaftLog.RaftResult
import org.nonsense.raft.RaftPeer.{CAMPAIGN_ELECTION, CAMPAIGN_PRE_ELECTION}
import org.nonsense.raft.ReadOnlyOption.Safe
import org.nonsense.raft.error.SnapshotTemporarilyUnavailable
import org.nonsense.raft.model.RaftPB
import org.nonsense.raft.model.RaftPB.{INVALID_ID, NodeId, TermT}
import org.nonsense.raft.protos.Protos
import org.nonsense.raft.protos.Protos._
import org.nonsense.raft.storage.Storage
import org.nonsense.raft.utils.{Err, Ok}

import scala.collection.JavaConverters._
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
  private val prs: mutable.Map[NodeId, Progress] = mutable.Map(),
  private var hasPendingConf: Boolean = false,
  private var leaderTransferee: Option[NodeId] = None,
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

  def nodes: Seq[NodeId]  = this.prs.keys.toSeq
  @inline def quorum: Int = this.prs.size / 2 + 1

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

    // handle vote message for all role
    msgType match {
      case MessageType.MsgRequestPreVote | MessageType.MsgRequestVote =>
        this.onMsgRequestPreVoteOrVote(msg)
        return
    }

    this.role match {
      case Follower =>
        msgType match {
          case MessageType.MsgAppend =>
            this.electionElapsed = 0
            this.leaderId = msg.getFrom
            this.handleAppendEntries(msg)
        }
      case PreCandidate | Candidate =>
        msgType match {
          case MessageType.MsgRequestPreVoteResponse | MessageType.MsgRequestVoteResponse =>
            this.onMsgRequestPreVoteOrVoteResponse(msg)
          case MessageType.MsgAppend =>
            this.becomeFollower(this.term, msg.getFrom)
            this.handleAppendEntries(msg)
        }

      case Leader =>
        msgType match {
          case MessageType.MsgAppendResponse =>
            if (!this.prs.contains(msg.getFrom)) {
              logger.debug("{} no progress available for {}", this.tag, msg.getFrom)
              return
            }
            this.handleAppendResponse(msg)
        }

    }
  }

  // ========================================================================= MsgRequestPreVoteOrVoteResponse
  private def onMsgRequestPreVoteOrVoteResponse(message: Protos.Message): RaftResult[Unit] = {
    assert(this.role == PreCandidate || this.role == Candidate)
    assert(
      message.getMsgType == MessageType.MsgRequestPreVoteResponse ||
        message.getMsgType == MessageType.MsgRequestVoteResponse)

    val curTerm = this.term
    // Only handle vote responses corresponding to our candidacy (while in
    // state Candidate, we may get stale MsgPreVoteResp messages in this term from
    // our pre-candidate state).
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
      for { peer <- this.prs.keys if peer != this.id } {
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
    val curTerm = this.term
    this.reset(curTerm)
    // I am the leader
    this.leaderId = this.id
    this.role = Leader

    // TODO: why check the uncommitted conf
    val begin = this.log.committed + 1
    val ents = this.log
      .entries(begin, RaftLog.NO_LIMIT)
      .unwrap("unexpected error getting uncommitted entries")
    val numOfConf = RaftPeer.numOfPendingConf(ents)
    if (numOfConf > 1) {
      throw new Exception(s"$tag unexpected double uncommitted config entry")
    } else if (numOfConf == 1) {
      this.hasPendingConf = true
    }

    // append a entry immediately
    this.appendEntry(mutable.Buffer(Entry.newBuilder()))
    logger.info("{} became leader at term {}", this.tag, this.term)

    // call bcast append even no peer exists.
    this.bcastAppend()
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

  // send append message or snapshot message to all peers of me
  private def bcastAppend(): Unit = {
    for {
      p <- this.prs.keys if p != this.id
    } {
      this.sendAppend(p)
    }
  }

  // send_append sends RPC, with entries to the given peer.
  private def sendAppend(to: RaftPB.NodeId): Unit = {
    val progress = this.prs(to)
    if (progress.isPaused) {
      return
    }

    val term = this.log.term(progress.nextIndex - 1)
    val ents = this.log.entries(progress.nextIndex, this.config.maxSizePerMsg)
    val msg: Message.Builder = if (term.isErr || ents.isErr) {
      // send snapshot if we failed to get term or entries
      prepareSendSnapshot(to) match {
        case None    => return
        case Some(m) => m
      }
    } else { // send entries
      this.prepareSendEntries(to, term.get, ents.get)
    }

    this.send(msg)
  }

  private def handleAppendEntries(req: Protos.Message): Unit = {
    assert(req.getMsgType == MessageType.MsgAppend)
    assert(this.role == Follower)

    val resp = Message
      .newBuilder()
      .setFrom(this.id)
      .setTo(req.getFrom)
      .setMsgType(MessageType.MsgAppendResponse)

    if (req.getIndex < this.log.committed) {
      resp.setIndex(this.log.committed)
    } else {
      this.log.maybeAppend(req.getIndex, req.getLogTerm, req.getCommit, req.getEntriesList.asScala) match {
        case Some(mlastIdx) =>
          resp.setIndex(mlastIdx)
        case None =>
          logger.debug(
            "{} [logterm: {}, index: {}] rejected msgApp [logterm: {}, index: {}] from {}",
            this.tag,
            this.log.term(req.getIndex).getOrElse(0),
            req.getIndex,
            req.getLogTerm,
            req.getIndex,
            req.getFrom
          )
          resp.setIndex(req.getIndex)
          resp.setReject(true)
          resp.setRejectHint(this.log.lastIndex)
      }
    }

    this.send(resp)
  }

  private def handleAppendResponse(resp: Protos.Message): Unit = {
    // TODO: check prs
    val pr = this.prs(resp.getFrom)
    pr.setRecentActive(true)

    // when rejected by follower
    if (resp.getReject) {
      logger.debug("{} received msgAppend rejection(lastindex: {}) from {} for index {}",
                   this.tag,
                   resp.getRejectHint,
                   resp.getFrom,
                   resp.getIndex)
      if (pr.maybeDecrTo(resp.getIndex, resp.getRejectHint)) {}
    } else {}
  }

  private def prepareSendEntries(
    to: RaftPB.NodeId,
    term: TermT,
    entries: mutable.Buffer[Protos.Entry]
  ): Message.Builder = {
    import scala.collection.JavaConverters._
    val progress = this.prs(to)
    val m = Message
      .newBuilder()
      .setTo(to)
      .setMsgType(MessageType.MsgAppend)
      .setIndex(progress.nextIndex - 1)
      .setLogTerm(term)
      .addAllEntries(entries.asJavaCollection)
      .setCommit(this.log.committed)

    if (m.getEntriesCount > 0) {
      progress.state match {
        case ReplicateState =>
          val lastEntryIdx = m.getEntriesBuilderList.get(m.getEntriesCount - 1).getIndex
          progress.optimisticUpdate(lastEntryIdx)
          progress.addInFlight(lastEntryIdx)
        case ProbeState => progress.pause()
        case s          => throw new Exception(s"$tag is sending append in unhandled state $s")
      }
    }
    m
  }

  private def prepareSendSnapshot(to: RaftPB.NodeId): Option[Message.Builder] = {
    val pr = this.prs(to)
    if (!pr.recentActive) {
      logger.debug("{} ignore sending snapshot to {} since it is not recently active", this.tag, to)
      return None
    }

    val m = Message.newBuilder().setMsgType(MessageType.MsgSnapshot).setTo(to)
    val snap: Snapshot = this.log.snapshot() match {
      case Err(SnapshotTemporarilyUnavailable) =>
        logger.debug(
          "{} failed to send snapshot to {} because snapshot is termporarily unavailable",
          this.tag,
          to)
        return None
      case Err(e) => throw new Exception(s"$tag unexpected error: $e")
      case Ok(sn) if sn.getMetadata.getIndex == 0 =>
        throw new Exception(s"$tag need non-empty snapshot")
      case Ok(sn) => sn
    }
    m.setSnapshot(snap)

    val (sidx, sterm) = (snap.getMetadata.getIndex, snap.getMetadata.getTerm)
    logger.debug(
      "{} [firstindex: {}, commit: {}] sent snapshot[index: {}, term: {}] to {} [{}]",
      this.tag,
      this.log.firstIndex,
      this.log.committed,
      sidx,
      sterm,
      to,
      pr
    )
    pr.becomeSnapshot(sidx)

    logger.debug("{} paused sending replication messages to {} [{}]", this.tag, to, pr)

    Some(m)
  }

  def addNode(id: NodeId): Unit = {
    // TODO: why set here
    this.hasPendingConf = false

    // only leader role needs to add new progress
    this.role match {
      case Leader if !this.prs.contains(id) =>
        val lastIdx = this.log.lastIndex
        this.prs(id) = Progress(0L, lastIdx + 1, this.config.maxInflightMegs)
      case Leader =>
      // Ignore any redundant addNode calls (which can happen because the
      // initial bootstrapping entries are applied twice).
      case r => logger.error("cannot add node in role {}", r)
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

    // clear candidate state
    this.votes.clear()

    // clear leader state
    val (lastIdx, maxInflight) = (this.log.lastIndex, this.config.maxInflightMegs)
    for { peer <- this.prs.keys } {
      val progress = Progress(0, lastIdx + 1, maxInflight)
      if (peer == this.id) {
        progress.matched = lastIdx
      }
      this.prs.update(peer, progress)
    }
    this.hasPendingConf = false
    this.leaderTransferee = None
    // TODO: check what's read only
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
    val prs = existedPeers.foldLeft(mutable.Map[NodeId, Progress]()) {
      case (b, p) =>
        b(p) = Progress(matched = 0, nextIdx = 1, inflightSize = config.maxInflightMegs)
        b
    }

    val follower = RaftPeer(
      log = raftLog,
      config = config,
      id = id,
      tag = tag,
      prs = prs,
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
