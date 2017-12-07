package org.nonsense.raft.storage

import org.nonsense.raft.error.StorageError
import org.nonsense.raft.model.RaftState
import org.nonsense.raft.protos.Protos.{Entry, Snapshot}
import org.nonsense.raft.storage.Storage.StorageResult
import org.nonsense.raft.utils.Result

trait Storage {

  def initialState(): StorageResult[RaftState]
  def entries(low: Long, high: Long, maxSize: Long): StorageResult[Vector[Entry]]
  def term(index: Long): StorageResult[Long]
  def firstIndex(): StorageResult[Long]
  def lastIndex(): StorageResult[Long]
  def snapshot(): StorageResult[Snapshot]
}

object Storage {
  type StorageResult[T] = Result[T, StorageError]
}
