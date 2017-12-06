package org.nonsense.raft

import org.nonsense.raft.model.RaftPB.NodeId

import scala.collection.mutable.{Map => MutableMap}

sealed trait RaftRole
case object Follower  extends RaftRole
case object Candidate extends RaftRole
case object Leader    extends RaftRole

object RaftRole {
  def apply: RaftRole = Follower
}

sealed trait RoleState
case class FollowerState() extends RoleState
case class CandidateState(
  votes: MutableMap[NodeId, Boolean] = MutableMap()
) extends RoleState
case class LeaderState(prs: MutableMap[NodeId, Progress]) extends RoleState
