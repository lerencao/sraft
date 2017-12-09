/**
  * Raft log abstraction
  */

package org.nonsense.raft
import org.nonsense.raft.RaftLog.RaftResult
import org.nonsense.raft.error.{Compacted, StorageError, Unavailable}
import org.nonsense.raft.model.RaftPB
import org.nonsense.raft.model.RaftPB.{IndexT, TermT}
import org.nonsense.raft.protos.Protos
import org.nonsense.raft.protos.Protos.Entry
import org.nonsense.raft.storage.Storage
import org.nonsense.raft.storage.Storage.StorageResult
import org.nonsense.raft.utils.{Err, Ok, Result}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class RaftLog[T <: Storage] private (
  store: T,
  unstable: Unstable,
  var committed: Long,
  var applied: Long,
  tag: String
) {

  def snapshot(): RaftResult[Protos.Snapshot] = this.unstable.snapshot match {
    case Some(s) => Ok(s)
    case None    => this.store.snapshot()
  }

  def maybeCommit(maxIdx: IndexT, term: TermT): Boolean = {
    // always commit log of my term
    if (maxIdx > this.committed && this.term(maxIdx).getOrElse(0) == term) {
      this.commitTo(maxIdx)
      true
    } else {
      false
    }
  }

  // maybe_append returns None if the entries cannot be appended. Otherwise,
  // it returns Some(last index of new entries).
  def maybeAppend(logIdx: IndexT,
                  logTerm: TermT,
                  committed: IndexT,
                  ents: mutable.Buffer[Entry]): Option[IndexT] = {
    if (!this.isTermMatch(logIdx, logTerm)) {
      return None
    }

    val conflictIdx = this.findConflict(ents)
    if (conflictIdx == 0) {
      // it's ok
    } else if (conflictIdx <= this.committed) {
      throw new Exception(
        s"${this.tag} entry $conflictIdx conflict with committed entry ${this.committed}")
    } else {
      assert(logIdx == ents.head.getIndex - 1)
      this.append(ents.iterator.drop((conflictIdx - ents.head.getIndex).toInt).toSeq)
    }

    val lastNewIdx = logIdx + ents.length
    this.commitTo(Math.min(committed, lastNewIdx))
    Some(lastNewIdx)
  }

  def commitTo(toCommit: RaftPB.IndexT): Unit = {
    if (toCommit > this.lastIndex) {
      throw new Exception(s"$tag to_commit {} is out of range [last_index ${this.lastIndex}]")
    } else if (toCommit <= this.committed) {
      // never decrease commit
    } else { // now this.commited < toCommit <= this.lastIndex
      this.committed = toCommit
    }
  }

  /**
    * make sure  lastIdx <= low <= high <= lastIdx + 1
    * @param low
    * @param high
    * @return err if not inbound
    */
  private def checkOutOfBounds(low: Long, high: Long) = {
    if (low > high) {
      throw new IllegalArgumentException(s"invalid slice $low > $high")
    }

    val firstIdx  = this.firstIndex
    val lastIndex = this.lastIndex
    if (low < firstIdx) {
      Some(Compacted)
    } else if (high > lastIndex + 1) {
      throw new IllegalArgumentException(s"slice[$low, $high] out of bound[$firstIdx, $lastIndex]")
    } else {
      None
    }
  }

  // is_up_to_date determines if the given (lastIndex,term) log is more up-to-date
  // by comparing the index and term of the last entry in the existing logs.
  // If the logs have last entry with different terms, then the log with the
  // later term is more up-to-date. If the logs end with the same term, then
  // whichever log has the larger last_index is more up-to-date. If the logs are
  // the same, the given log is up-to-date.
  def isUpToDate(idx: IndexT, term: TermT): Boolean =
    term > this.lastTerm || (term == this.lastTerm && idx >= this.lastIndex)

  /**
    * append entries, truncate if necessary
    * @param ents to be append
    * @return last index of the log
    */
  def append(ents: Seq[Entry]): IndexT = {
    if (ents.nonEmpty) {
      if (ents.head.getIndex <= this.committed) {
        throw new Exception(
          s"$tag entry head ${ents.head.getIndex} is out of range (committed ${this.committed} ..]")
      }
      this.unstable.truncateAndAppend(ents)
    }

    this.lastIndex
  }

  /**
    * get entry slice,
    * @param low inclusive
    * @param high exclusive
    * @param maxSize max size
    * @return
    */
  def slice(low: IndexT, high: IndexT, maxSize: Long): StorageResult[mutable.Buffer[Entry]] = {
    val err = this.checkOutOfBounds(low, high)
    if (err.nonEmpty) {
      return Err(err.get)
    }

    if (low == high) {
      return Ok(new ArrayBuffer[Entry](0))
    }

    val unstableOffset           = this.unstable.offset
    var ents: ArrayBuffer[Entry] = new ArrayBuffer[Entry](16)

    if (low < unstableOffset) {
      val stableHigh = Math.min(high, unstableOffset)
      val stableEnts = this.store.entries(low, stableHigh, maxSize)
      stableEnts match {
        case Err(e @ Compacted) => return Err(e)
        case Err(Unavailable) =>
          throw new Exception(s"entries[$low:$stableHigh] is unavailabe from storage")
        case Err(e) => throw new Exception(s"unexpected error: $e")
        case _      =>
      }
      ents.appendAll(stableEnts.get)
      // it means that the maxSize is reached, we can return now
      if (ents.length < stableHigh - low) {
        return Ok(ents)
      }
    }

    if (high > unstableOffset) {
      val unstableLow              = Math.max(low, unstableOffset)
      val unstableEnts: Seq[Entry] = this.unstable.slice(unstableLow, high)
      ents.appendAll(unstableEnts)
    }

    RaftPB.limitSize(ents, maxSize)
    Ok(ents)
  }

  def entries(idx: IndexT, maxSize: Long): RaftResult[mutable.Buffer[Entry]] = {
    val lastIdx = this.lastIndex
    if (idx > lastIdx) {
      Ok(mutable.Buffer())
    } else {
      this.slice(idx, lastIdx + 1, maxSize)
    }
  }

  def firstIndex: Long = {
    unstable.maybeFirstIndex() match {
      case Some(idx) => idx
      case None      => this.store.firstIndex().ok.get
    }
  }

  def lastIndex: Long = {
    unstable.maybeLastIndex() match {
      case Some(idx) => idx
      case None      => this.store.lastIndex().ok.get
    }
  }

  def lastTerm: Long = {
    this.term(this.lastIndex) match {
      case Ok(t) => t
      case Err(e) =>
        throw new RuntimeException(s"unexpected error when getting the last term: $e")
    }
  }

  def term(idx: Long): RaftResult[Long] = {
    val dummyIdx = this.firstIndex - 1
    if (idx < dummyIdx || idx > this.lastIndex) {
      Ok(0L)
    } else {
      unstable.maybeTerm(idx) match {
        case Some(term) => Ok(term)
        case None       => this.store.term(idx)
      }
    }
  }

  def isTermMatch(idx: Long, term: Long): Boolean = this.term(idx).ok.contains(term)

  // find_conflict finds the index of the conflict.
  // It returns the first index of conflicting entries between the existing
  // entries and the given entries, if there are any.
  // If there is no conflicting entries, and the existing entries contain
  // all the given entries, zero will be returned.
  // If there is no conflicting entries, but the given entries contains new
  // entries, the index of the first new entry will be returned.
  // An entry is considered to be conflicting if it has the same index but
  // a different term.
  // The first entry MUST have an index equal to the argument 'from'.
  // The index of the given entries MUST be continuously increasing.
  def findConflict(ents: Seq[Entry]): Long = {
    for (e <- ents) {
      if (!this.isTermMatch(e.getIndex, e.getTerm)) {
        if (e.getIndex <= this.lastIndex) {
          // TODO: log this case
          // s"found conflict at index ${e.index}, [existing term: ${this.term(e.index).left.getOrElse(0)}, conflicting term: ${e.term}]"
        }
        return e.getIndex
      }
    }

    0
  }

  def applyTo(idx: Long): Unit = {
    if (idx > 0) {
      if (this.applied <= idx && idx <= this.committed) {
        this.applied = idx
      } else {
        throw new RuntimeException(
          s"$tag applied($idx) is out of range [prev_applied(${this.applied}), committed(${this.committed})]")
      }
    }
  }
}

object RaftLog {
  val NO_LIMIT = Long.MaxValue
  type RaftResult[T] = Result[T, StorageError]

  /**
    *
    * @param store storage
    * @param applied should be great than 0
    * @param tag string tag
    * @tparam T storage type
    * @return
    */
  def apply[T <: Storage](store: T, applied: Long, tag: String): RaftLog[T] = {
    assert(applied >= 0)
    val firstIndex = store.firstIndex().get
    val lastIndex  = store.lastIndex().get
    val rs         = store.initialState().get

    val log = RaftLog(
      store = store,
      tag = tag,
      committed = rs.hardState.getCommit,
      applied = firstIndex - 1,
      unstable = Unstable(offset = lastIndex + 1, tag = tag)
    )
    log.applyTo(applied)
    log
  }
}
