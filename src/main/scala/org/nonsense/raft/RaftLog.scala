/**
  * Raft log abstraction
  */

package org.nonsense.raft
import org.nonsense.raft.RaftLog.RaftResult
import org.nonsense.raft.error.StorageError
import org.nonsense.raft.protos.Protos.Entry
import org.nonsense.raft.storage.Storage
import org.nonsense.raft.utils.{Err, Ok, Result}

case class RaftLog[T <: Storage] private (
  store: T,
  unstable: Unstable,
  var committed: Long,
  var applied: Long,
  tag: String
) {
  def append(ents: Seq[Entry]) = ???

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
