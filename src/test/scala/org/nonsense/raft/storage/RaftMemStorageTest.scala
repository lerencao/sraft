package org.nonsense.raft.storage

import org.nonsense.raft.error.{Compacted, Unavailable}
import org.nonsense.raft.protos.Protos.Entry

class RaftMemStorageTest extends UnitSpec with ProtoHelper {
  test("storage term") {
    val ents: Vector[Entry] = Range(3, 6).map(r => newEntry(r, r)).toVector

    val tests = List(
      (2, Right(Compacted)),
      (3, Left(3)),
      (4, Left(4)),
      (5, Left(5)),
      (6, Right(Unavailable))
    )

    for (elem <- tests) {
      val storage = new RaftMemStorage(ents)
      val t       = storage.term(elem._1)
      assert(t == elem._2)
    }
  }

  test("storage entries") {
    val ents: Vector[Entry] = Range(3, 7).map(r => newEntry(r, r)).toVector

    val test = List(
      (2, 6, Long.MaxValue, Right(Compacted)),
      (3, 4, Long.MaxValue, Right(Compacted)),
      (4, 5, Long.MaxValue, Left(ents.slice(1, 2))),
      (4, 6, Long.MaxValue, Left(ents.slice(1, 3)))
    )

    for ((low, high, maxSize, expected) <- test) {
      val storage = new RaftMemStorage(ents)
      val actual  = storage.entries(low, high, maxSize)
      assert(actual == expected)
    }
  }
}
