package org.nonsense.raft.storage

import com.google.protobuf.ByteString
import org.nonsense.raft.error.{Compacted, EntryMissing, SnapshotOutofDate, Unavailable}
import org.nonsense.raft.model.RaftState
import org.nonsense.raft.protos.Protos._
import org.nonsense.raft.storage.Storage.StorageResult
import org.nonsense.raft.utils.{Err, Ok}

class RaftMemStorage(
  private var entries: Vector[Entry] = Vector(),
  private var hardState: HardState = HardState.getDefaultInstance
) extends Storage {
  private var snap: Snapshot                       = Snapshot.getDefaultInstance
  override def snapshot(): StorageResult[Snapshot] = Ok(snap)

  override def initialState(): StorageResult[RaftState] = {
    val rs = RaftState(hardState = hardState, confState = snap.getMetadata.getConfState)
    Ok(rs)
  }

  override def entries(low: Long, high: Long, maxSize: Long): StorageResult[Vector[Entry]] = {
    val offset = entries(0).getIndex
    if (low <= offset) {
      Err(Compacted)
    } else if (high > innerLastIndex) {
      throw new IndexOutOfBoundsException
    } else {
      val (lo, hi): (Int, Int) = ((low - offset).toInt, (high - offset).toInt)
      val ents                 = entries.slice(lo, hi)
      // TODO: limit entries
      Ok(ents)
    }
  }

  override def term(index: Long): StorageResult[Long] = {
    val offset = entries(0).getIndex
    if (index < offset) {
      Err(Compacted)
    } else {
      val i = (index - offset).toInt
      if (i >= entries.length) {
        Err(Unavailable)
      } else {
        Ok(entries(i).getTerm)
      }
    }

  }

  override def firstIndex(): StorageResult[Long] = {
    Ok(innerFirstIndex)
  }

  override def lastIndex(): StorageResult[Long] = Ok(innerLastIndex)

  private def innerFirstIndex: Long = entries(0).getIndex + 1
  private def innerLastIndex: Long  = entries(0).getIndex + entries.length - 1

  def append(ents: Vector[Entry]): StorageResult[Unit] = {
    ents.length match {
      case 0                                                            => Ok(Unit)
      case _ if ents.head.getIndex > innerLastIndex                     => Err(EntryMissing)
      case elen: Int if ents.head.getIndex + elen - 1 < innerFirstIndex => Ok(Unit)
      case _ =>
        val first = ents.head.getIndex
        val toAppend = if (first < innerFirstIndex) {
          ents.slice((innerFirstIndex - first).toInt, ents.length)
        } else {
          ents
        }
        val offset = toAppend(0).getIndex - entries(0).getIndex
        entries = entries.slice(0, offset.toInt) ++ toAppend
        Ok(Unit)
    }
  }

  def compact(compactIndex: Long): StorageResult[Unit] = {
    val offset = entries(0).getIndex
    if (compactIndex <= offset) {
      Err(Compacted)
    } else if (compactIndex > innerLastIndex) {
      throw new IndexOutOfBoundsException(
        s"compact $compactIndex is out of bound lastindex($innerLastIndex)")
    } else {
      val i = (compactIndex - offset).toInt
      entries = entries.drop(i)
      Ok(Unit)
    }
  }

  def createSnapshot(idx: Long,
                     confState: Option[ConfState],
                     data: Array[Byte]): StorageResult[Snapshot] = {
    if (idx <= snap.getMetadata.getIndex) {
      Err(SnapshotOutofDate)
    } else if (idx > innerLastIndex) {
      throw new IndexOutOfBoundsException(
        s"snapshot $idx is out of bound lastindex($innerLastIndex)")
    } else {
      val offset    = idx - entries(0).getIndex
      val termOfIdx = entries(offset.toInt).getTerm
      val meta      = SnapshotMetadata.newBuilder(snap.getMetadata).setIndex(idx).setTerm(termOfIdx)
      for (cs <- confState) {
        meta.setConfState(cs)
      }

      this.snap =
        Snapshot.newBuilder(snap).setData(ByteString.copyFrom(data)).setMetadata(meta).build()

      snapshot()
    }
  }

  def applySnapshot(snapshot: Snapshot): StorageResult[Unit] = {
    val snapIndex = snapshot.getMetadata.getIndex
    if (snapIndex <= this.snap.getMetadata.getIndex) {
      Err(SnapshotOutofDate)
    } else {
      val e = Entry.newBuilder().setTerm(snapshot.getMetadata.getTerm).setIndex(snapIndex).build()
      this.entries = Vector(e)
      this.snap = snapshot
      Ok(Unit)
    }
  }
}
