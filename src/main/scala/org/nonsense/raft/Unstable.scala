package org.nonsense.raft

import org.nonsense.raft.protos.Protos.{Entry, Snapshot}

import scala.collection.mutable.ArrayBuffer

case class Unstable(
  var snapshot: Option[Snapshot] = None,
  var offset: Long = 0,
  // buffer is mutable
  entries: ArrayBuffer[Entry] = ArrayBuffer(),
  tag: String = ""
) {

  def maybeFirstIndex(): Option[Long] = {
    snapshot.map(s => s.getMetadata.getIndex + 1)
  }

  def maybeLastIndex(): Option[Long] = {
    entries.length match {
      case 0        => snapshot.map(s => s.getMetadata.getIndex)
      case len: Int => Some(offset + len - 1)
    }
  }

  def maybeTerm(idx: Long): Option[Long] = {
    if (idx < offset) {
      for (s <- snapshot if s.getMetadata.getIndex == idx) yield s.getMetadata.getTerm
    } else {
      for (last <- maybeLastIndex() if idx <= last) yield entries((idx - offset).toInt).getTerm
    }
  }

  def stableTo(idx: Long, term: Long): Unit = {
    maybeTerm(idx) match {
      case None =>
      case Some(t) if t == term && idx >= offset =>
        val dropNum = idx - offset + 1
        this.entries.trimStart(dropNum.toInt)
        this.offset = idx + 1
      case _ =>
    }
  }

  def stableSnapshotTo(idx: Long): Unit = {
    snapshot match {
      case Some(snap) if snap.getMetadata.getIndex == idx =>
        this.snapshot = None
      case _ =>
    }
  }

  def restore(snap: Snapshot): Unit = {
    this.entries.clear()
    this.offset = snap.getMetadata.getIndex + 1
    this.snapshot = Some(snap)
  }

  def truncateAndAppend(ents: Seq[Entry]): Unit = {
    val after = ents.head.getIndex
    if (after <= this.offset) {
      this.offset = after
      this.entries.clear()
      this.entries.appendAll(ents)
    } else if (after <= (this.offset + this.entries.length)) {
      val truncateNum = this.entries.length - (after - this.offset)
      this.entries.trimEnd(truncateNum.toInt)
      this.entries.appendAll(ents)
    } else {
      throw new IndexOutOfBoundsException(
        s"$after is out of bound [$offset, ${offset + entries.length})")
    }
  }

  def slice(low: Long, high: Long): Seq[Entry] = {
    if (low > high) {
      throw new IllegalArgumentException(s"invalid slice: $low > $high")
    }

    val s = low - this.offset
    val e = high - this.offset

    entries.slice(s.toInt, e.toInt)
  }

  /**
    * [low, high)
    * @param low inclusive low
    * @param high exclusive high
    */
  private def checkOutOfBound(low: Long, high: Long): Unit = {
    if (low > high) {
      throw new IllegalArgumentException(s"invalid slice: $low > $high")
    }

    val upper = this.offset + this.entries.length

    if (low < this.offset || high > upper) {
      throw new IndexOutOfBoundsException(s"[$low, $high) is out of bound [$offset, $upper)")
    }
  }
}
