package org.nonsense.raft.storage

import org.nonsense.raft.protos.Protos.{Entry, Snapshot, SnapshotMetadata}

trait ProtoHelper {

  def newEntry(index: Int, term: Int): Entry = {
    Entry.newBuilder().setIndex(index).setTerm(term).build()
  }

  def newSnapshot(index: Int, term: Int): Snapshot = {
    val meta = SnapshotMetadata.newBuilder().setIndex(index).setTerm(term).build()
    Snapshot.newBuilder().setMetadata(meta).build()
  }
}
