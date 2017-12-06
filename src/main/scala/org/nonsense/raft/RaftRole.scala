package org.nonsense.raft

sealed trait RaftRole
case object Follower  extends RaftRole
case object Candidate extends RaftRole
case object Leader    extends RaftRole

object RaftRole {
  def apply: RaftRole = Follower
}
