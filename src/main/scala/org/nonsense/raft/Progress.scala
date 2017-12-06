package org.nonsense.raft

import org.nonsense.raft.model.RaftPB.IndexT

import scala.collection.mutable.{ArrayBuffer, Buffer => MutableBuffer}

sealed trait ProcessState
case object Probe     extends ProcessState
case object Replicate extends ProcessState
case object Snapshot  extends ProcessState

object ProcessState {
  def default: ProcessState = Probe
}

case class Progress(
  var matched: IndexT = 0,
  var nextIndex: IndexT = 0,
  var state: ProcessState,
  var paused: Boolean = false,
  var pendingSnap: IndexT,
  var recentActive: Boolean,
  var ins: Inflights
) {}

class Inflights private (
  // the starting index in the buffer
  var start: Int,
  // number of inflights in the buffer
  var count: Int,
  cap: Int,
  buffer: MutableBuffer[IndexT]
) {
  def full: Boolean = this.count == this.cap

  def add(inflight: IndexT): Unit = {
    if (this.full) {
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
