package org.nonsense.raft

import org.nonsense.raft.model.RaftPB.IndexT

import scala.collection.mutable.{ArrayBuffer, Buffer => MutableBuffer}

sealed trait ProgressState
case object ProbeState     extends ProgressState
case object ReplicateState extends ProgressState
case object SnapshotState  extends ProgressState

object ProgressState {
  def default: ProgressState = ProbeState
}

object Progress {

  def apply(matched: IndexT, nextIdx: IndexT, inflightSize: Int): Progress =
    new Progress(matched = matched, nextIndex = nextIdx, ins = Inflights(inflightSize))
}

class Progress(
  var matched: IndexT = 0,
  var nextIndex: IndexT = 0,
  // When in ProgressStateProbe, leader sends at most one replication message
  // per heartbeat interval. It also probes actual progress of the follower.
  //
  // When in ProgressStateReplicate, leader optimistically increases next
  // to the latest entry sent after sending replication message. This is
  // an optimized state for fast replicating log entries to the follower.
  //
  // When in ProgressStateSnapshot, leader should have sent out snapshot
  // before and stops sending any replication message.
  var state: ProgressState = ProgressState.default,
  // Paused is used in ProgressStateProbe.
  // When Paused is true, raft should pause sending replication message to this peer.
  var paused: Boolean = false,
  // pending_snapshot is used in ProgressStateSnapshot.
  // If there is a pending snapshot, the pendingSnapshot will be set to the
  // index of the snapshot. If pendingSnapshot is set, the replication process of
  // this Progress will be paused. raft will not resend snapshot until the pending one
  // is reported to be failed.
  var pendingSnap: IndexT = 0,
  // recent_active is true if the progress is recently active. Receiving any messages
  // from the corresponding follower indicates the progress is active.
  // RecentActive can be reset to false after an election timeout.
  var recentActive: Boolean = false,
  // Inflights is a sliding window for the inflight messages.
  // When inflights is full, no more message should be sent.
  // When a leader sends out a message, the index of the last
  // entry should be added to inflights. The index MUST be added
  // into inflights in order.
  // When a leader receives a reply, the previous inflights should
  // be freed by calling inflights.freeTo.
  private var ins: Inflights
) {

  def reset(state: ProgressState): Unit = {
    this.state = state
    this.paused = false
    this.pendingSnap = 0
    this.ins.reset()
  }

  def becomeProbe(): Unit = {
    // If the original state is ProgressStateSnapshot, progress knows that
    // the pending snapshot has been sent to this peer successfully, then
    // probes from pendingSnapshot + 1.
    this.state match {
      case SnapshotState =>
        this.reset(ProbeState)
        this.nextIndex = Math.max(this.matched + 1, this.pendingSnap + 1)
      case _ =>
        this.reset(ProbeState)
        this.nextIndex = this.matched + 1
    }
  }

  def becomeSnapshot(snapIdx: IndexT): Unit = {
    this.reset(SnapshotState)
    this.pendingSnap = snapIdx
  }

  def becomeReplicate(): Unit = {
    this.reset(ReplicateState)
    this.nextIndex = this.matched + 1
  }

  def snapshotFailure(): Unit = {
    this.pendingSnap = 0
  }

  // maybe_snapshot_abort unsets pendingSnapshot if Match is equal or higher than the pendingSnapshot
  // TODO: review this comment
  def maybeSnapshotAbort(): Boolean = {
    this.state == SnapshotState && this.matched >= this.pendingSnap
  }

  // maybe_update returns false if the given n index comes from an outdated message.
  // Otherwise it updates the progress and returns true.
  def maybeUpdate(n: IndexT): Boolean = {
    val needUpdate = this.matched < n
    if (needUpdate) {
      this.matched = n
      this.resume()
    }

    if (this.nextIndex < n + 1) {
      this.nextIndex = n + 1
    }

    needUpdate
  }

  def optimisticUpdate(n: IndexT): Unit = {
    this.nextIndex = n + 1
  }

  def addInFlight(idx: IndexT): Unit = {
    this.ins.add(idx)
  }

  // maybe_decr_to returns false if the given to index comes from an out of order message.
  // Otherwise it decreases the progress next index to min(rejected, last) and returns true.
  def maybeDecrTo(rejected: IndexT, last: IndexT): Boolean = {
    this.state match {
      case ReplicateState =>
        // the rejection must be stale if the progress has matched and "rejected"
        // is smaller than "match".
        if (rejected <= this.matched) {
          false
        } else {
          this.nextIndex = this.matched + 1
          true
        }
      case _ =>
        // the rejection must be stale if "rejected" does not match next - 1
        if (this.nextIndex == 0 || this.nextIndex - 1 != rejected) {
          false
        } else {
          this.nextIndex = Math.min(rejected, last + 1)
          if (this.nextIndex < 1) {
            this.nextIndex = 1
          }
          this.resume()
          true
        }
    }
  }

  def pause() { this.paused = true }

  def resume() { this.paused = false }

  def isPaused: Boolean = this.state match {
    case ProbeState     => this.paused
    case ReplicateState => this.ins.isFull
    case SnapshotState  => true
  }
}

class Inflights private (
  // the starting index in the buffer
  var start: Int,
  // number of inflights in the buffer
  var count: Int,
  cap: Int,
  buffer: MutableBuffer[IndexT]
) {
  def isFull: Boolean = this.count == this.cap

  def add(inflight: IndexT): Unit = {
    if (this.isFull) {
      throw new Exception("cannot add into a full inflights")
    }

    var next: Int = this.start + this.count
    if (next >= this.cap) {
      next -= this.cap
    }

    assert(next <= this.buffer.length)
    if (next == this.buffer.length) {
      this.buffer.append(inflight)
    } else {
      this.buffer.update(next, inflight)
    }
    this.count += 1
  }

  /**
    * frees the inflights smaller or equal to the given flight.
    * @param to given flight
    */
  def freeTo(to: IndexT): Unit = {
    if (this.count == 0 || to < this.buffer(this.start)) {}
    var (i, idx) = (0, start)
    var found    = false
    while (i < this.count && !found) {
      if (to < this.buffer(idx)) {
        found = true
      }

      if (!found) {
        idx += 1
        if (idx >= this.cap) {
          idx -= this.cap
        }
        i += 1
      }
    }

    // free first i inflights and set new start index
    this.count -= i
    this.start = idx
  }

  def freeFirst(): Unit = {
    if (count > 0) {
      val f = this.buffer(this.start)
      this.freeTo(f)
    }
  }

  def reset(): Unit = {
    this.count = 0
    this.start = 0
  }
}

object Inflights {

  def apply(cap: Int): Inflights = new Inflights(
    start = 0,
    count = 0,
    cap = cap,
    buffer = new ArrayBuffer(cap)
  )
}
