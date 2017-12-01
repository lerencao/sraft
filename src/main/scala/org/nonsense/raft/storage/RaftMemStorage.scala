package org.nonsense.raft.storage

import com.google.protobuf.ByteString
import org.nonsense.raft.error.{Compacted, EntryMissing, SnapshotOutofDate, Unavailable}
import org.nonsense.raft.model.RaftState
import org.nonsense.raft.protos.Protos._
import org.nonsense.raft.storage.Storage.Result

class RaftMemStorage(
    private var entries: Vector[Entry] = Vector(),
    private var hardState: HardState = HardState.getDefaultInstance
) extends Storage {
  private var snap: Snapshot                = Snapshot.getDefaultInstance
  override def snapshot(): Result[Snapshot] = Left(snap)

  override def initialState(): Result[RaftState] = {
    val rs = RaftState(hardState = hardState, confState = snap.getMetadata.getConfState)
    Left(rs)
  }

  override def entries(low: Long, high: Long, maxSize: Long): Result[Vector[Entry]] = {
    val offset = entries(0).getIndex
    if (low <= offset) {
      Right(Compacted)
    } else if (high > innerLastIndex) {
      throw new IndexOutOfBoundsException
    } else {
      val (lo, hi): (Int, Int) = ((low - offset).toInt, (high - offset).toInt)
      val ents                 = entries.slice(lo, hi)
      // TODO: limit entries
      Left(ents)
    }
  }

  override def term(index: Long): Result[Long] = {
    val offset = entries(0).getIndex
    if (index < offset) {
      Right(Compacted)
    } else {
      val i = (index - offset).toInt
      if (i >= entries.length) {
        Right(Unavailable)
      } else {
        Left(entries(i).getTerm)
      }
    }

  }

  override def firstIndex(): Left[Long, Nothing] = {
    Left(innerFirstIndex)
  }

  override def lastIndex(): Left[Long, Nothing] = Left(innerLastIndex)

  private def innerFirstIndex: Long = entries(0).getIndex + 1
  private def innerLastIndex: Long  = entries(0).getIndex + entries.length - 1

  def append(ents: Vector[Entry]): Result[Unit] = {
    ents.length match {
      case 0 => Left(Unit)
      case _ if ents.head.getIndex > innerLastIndex => Right(EntryMissing)
      case elen: Int if ents.head.getIndex + elen - 1 < innerFirstIndex => Left(Unit)
      case _ =>
        val first = ents.head.getIndex
        val toAppend = if (first < innerFirstIndex) {
          ents.slice((innerFirstIndex - first).toInt, ents.length)
        } else {
          ents
        }
        val offset = toAppend(0).getIndex - entries(0).getIndex
        entries = entries.slice(0, offset.toInt) ++ toAppend
        Left(Unit)
    }
  }

  def compact(compactIndex: Long): Result[Unit] = {
    val offset = entries(0).getIndex
    if (compactIndex <= offset) {
      Right(Compacted)
    } else if (compactIndex > innerLastIndex) {
      throw new IndexOutOfBoundsException(s"compact $compactIndex is out of bound lastindex($innerLastIndex)")
    } else {
      val i = (compactIndex - offset).toInt
      entries = entries.drop(i)
      Left(Unit)
    }
  }

  def createSnapshot(idx: Long, confState: Option[ConfState], data: Array[Byte]): Result[Snapshot] = {
    if (idx <= snap.getMetadata.getIndex) {
      Right(SnapshotOutofDate)
    } else if (idx > innerLastIndex) {
      throw new IndexOutOfBoundsException(s"snapshot $idx is out of bound lastindex($innerLastIndex)")
    } else {
      val offset    = idx - entries(0).getIndex
      val termOfIdx = entries(offset.toInt).getTerm
      val meta      = SnapshotMetadata.newBuilder(snap.getMetadata).setIndex(idx).setTerm(termOfIdx)
      for (cs <- confState) {
        meta.setConfState(cs)
      }

      this.snap = Snapshot.newBuilder(snap).setData(ByteString.copyFrom(data)).setMetadata(meta).build()

      snapshot()
    }
  }

  def applySnapshot(snapshot: Snapshot): Result[Unit] = {
    val snapIndex = snapshot.getMetadata.getIndex
    if (snapIndex <= this.snap.getMetadata.getIndex) {
      Right(SnapshotOutofDate)
    } else {
      val e = Entry.newBuilder().setTerm(snapshot.getMetadata.getTerm).setIndex(snapIndex).build()
      this.entries = Vector(e)
      this.snap = snapshot
      Left(Unit)
    }
  }
}
