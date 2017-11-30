package org.nonsense.raft.storage

import org.nonsense.raft.error.{Compacted, EntryMissing, SnapshotOutofDate, Unavailable}
import org.nonsense.raft.model.RaftPB._
import org.nonsense.raft.model.Storage

class RaftMemStorage extends Storage {
  private var hardState: HardState = HardState()
  private var entries: Vector[Entry] = Vector()

  private var snap: Snapshot = Snapshot()
  override def snapshot(): Result[Snapshot] = Left(snap)

  override def initialState(): Result[RaftState] = {
    val rs = RaftState(hardState = hardState, confState = snap.metadata.confState)
    Left(rs)
  }

  override def entries(low: Long, high: Long, maxSize: Long): Result[Vector[Entry]] = {
    val offset = entries(0).index
    if(low <= offset) {
      Right(Compacted)
    }
    if (high > innerLastIndex) {
      throw new IndexOutOfBoundsException
    }

    val (lo, hi): (Int, Int) = ((low - offset).toInt, (high - offset).toInt)
    val ents = entries.slice(lo, hi)
    // TODO: limit entries
    Left(ents)
  }

  override def term(index: Long): Result[Long] = {
    val offset = entries(0).index
    if (index < offset) {
      return Right(Compacted)
    }

    val i = (index - offset).toInt
    if (i >= entries.length) {
      return Right(Unavailable)
    }

    val term = entries(i).term
    Left(term)
  }

  override def firstIndex(): Left[Long, Nothing] = {
    Left(innerFirstIndex)
  }

  override def lastIndex(): Left[Long, Nothing] = Left(innerLastIndex)

  private def innerFirstIndex: Long = entries(0).index + 1
  private def innerLastIndex: Long = entries(0).index + entries.length - 1

  def append(ents: Vector[Entry]): Result[Void] = {
    if (ents.isEmpty) {
      return Left(Void)
    }

    val first = ents(0).index
    val last = ents(0).index + ents.length - 1
    if (last < innerFirstIndex) {
      Left(Void)
    } else if (first > innerLastIndex) {
      Right(EntryMissing)
    } else {
      val toAppend = if (first < innerFirstIndex) {
        ents.slice((innerFirstIndex - first).toInt, ents.length)
      } else {
        ents
      }
      val offset = toAppend(0).index - entries(0).index
      entries = entries.slice(0, offset.toInt) ++ toAppend
      Left(Void)
    }
  }

  def compact(compactIndex: Long): Result[Void] = {
    val offset = entries(0).index
    if (compactIndex <= offset) {
      Right(Compacted)
    } else if (compactIndex > innerLastIndex){
      throw new IndexOutOfBoundsException(s"compact $compactIndex is out of bound lastindex($innerLastIndex)")
    } else {
      val i = (compactIndex - offset).toInt
      entries = entries.drop(i)
      Left(Void)
    }
  }

  def createSnapshot(idx: Long, confState: Option[ConfState], data: Array[Byte]): Result[Snapshot] = {
    if (idx <= snap.metadata.index) {
      Right(SnapshotOutofDate)
    } else if (idx > innerLastIndex) {
      throw new IndexOutOfBoundsException(s"snapshot $idx is out of bound lastindex($innerLastIndex)")
    } else {
      val offset = idx - entries(0).index
      val termOfIdx = entries(offset.toInt).term
      val meta = snap.metadata.copy(index = idx, term = termOfIdx)
      snap = snap.copy(data = data, confState.map(c => meta.copy(c)).getOrElse(meta))
      snapshot()
    }
  }

  def applySnapshot(snapshot: Snapshot): Result[Void] = {
    val snapIndex = snapshot.metadata.index
    if (snapIndex <= snap.metadata.index) {
      Right(SnapshotOutofDate)
    } else {
      val e = Entry(term = snapshot.metadata.term, index = snapIndex)
      this.entries = Vector(e)
      this.snap = snapshot
      Left(Void)
    }
  }
}

