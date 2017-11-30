package org.nonsense.raft.model

import org.nonsense.raft.StateRole

case class SoftState(leaderId: Long, stateRole: StateRole)
