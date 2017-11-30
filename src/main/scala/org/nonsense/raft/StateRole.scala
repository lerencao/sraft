package org.nonsense.raft

sealed trait StateRole
case object Follower extends StateRole
case object Candidate extends StateRole
case object Leader extends StateRole

object StateRole {
  def apply: StateRole = Follower
}
