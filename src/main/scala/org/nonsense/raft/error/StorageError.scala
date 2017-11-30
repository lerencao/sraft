package org.nonsense.raft.error

trait StorageError {
  def desc: String
  def cause: Option[Error] = None
}

case object Compacted extends StorageError {
  val desc = "log compacted"
}
case object Unavailable extends StorageError {
  val desc = "log unavailable"
}

case object SnapshotOutofDate extends StorageError {
  val desc = "snapshot out of date"
}

case object EntryMissing extends StorageError {
  val desc = "missing log entry"
}
case object SnapshotTemporarilyUnavailable extends StorageError {
  val desc = "snapshot is temporarily unavailable"
}

case class Other(error: Error) extends StorageError {
  val desc: String = error.getMessage
  override val cause = Some(error)
}
