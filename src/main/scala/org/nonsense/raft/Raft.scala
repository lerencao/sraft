package org.nonsense.raft

import org.nonsense.raft.model.RaftPB.NodeId
import org.nonsense.raft.storage.Storage

class Raft[T <: Storage](
  val store: T,
  val config: Config
) {
  val rs = store.initialState().left.get

}

sealed case class Config(
  id: Long,
  peers: Vector[NodeId],
  electionTick: Int
)

object Raft {
  def apply[T <: Storage](store: T, config: Config): Raft[T] = new Raft(store, config)
}
