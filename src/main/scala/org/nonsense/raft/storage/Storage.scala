package org.nonsense.raft.storage

import org.nonsense.raft.error.StorageError
import org.nonsense.raft.model.RaftState
import org.nonsense.raft.protos.Protos.{Entry, Snapshot}
import org.nonsense.raft.storage.Storage.Result

trait Storage {

  def initialState(): Result[RaftState]
  def entries(low: Long, high: Long, maxSize: Long): Result[Vector[Entry]]
  def term(index: Long): Result[Long]
  def firstIndex(): Result[Long]
  def lastIndex(): Result[Long]
  def snapshot(): Result[Snapshot]
}

object Storage {
  type Result[T] = Either[T, StorageError]
}
