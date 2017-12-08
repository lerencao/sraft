package org.nonsense.raft

import scala.collection.mutable.{Map => MutableMap}

sealed trait RaftRole
case object Follower     extends RaftRole
case object PreCandidate extends RaftRole
case object Candidate    extends RaftRole
case object Leader       extends RaftRole

object RaftRole {
  val default: RaftRole = Follower
}
