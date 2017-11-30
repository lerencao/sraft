package org.nonsense.raft.storage

import org.nonsense.raft.error.StorageError
import org.nonsense.raft.model.RaftPB.{Entry, RaftState, Snapshot}

trait Storage {
  type Result[T] = Either[T, StorageError]
  def initialState(): Result[RaftState]
  def entries(low: Long, high: Long, maxSize: Long): Result[Vector[Entry]]
  def term(index: Long): Result[Long]
  def firstIndex(): Result[Long]
  def lastIndex(): Result[Long]
  def snapshot(): Result[Snapshot]
}
