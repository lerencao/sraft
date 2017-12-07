package org.nonsense.raft.storage

import org.nonsense.raft.error.{Compacted, Unavailable}
import org.nonsense.raft.protos.Protos.Entry
import org.nonsense.raft.utils.{Err, Ok}

class RaftMemStorageTest extends UnitSpec with ProtoHelper {
  test("storage term") {
    val ents: Vector[Entry] = Range(3, 6).map(r => newEntry(r, r)).toVector

    val tests = List(
      (2, Err(Compacted)),
      (3, Ok(3)),
      (4, Ok(4)),
      (5, Ok(5)),
      (6, Err(Unavailable))
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
      (2, 6, Long.MaxValue, Err(Compacted)),
      (3, 4, Long.MaxValue, Err(Compacted)),
      (4, 5, Long.MaxValue, Ok(ents.slice(1, 2))),
      (4, 6, Long.MaxValue, Ok(ents.slice(1, 3)))
    )

    for ((low, high, maxSize, expected) <- test) {
      val storage = new RaftMemStorage(ents)
      val actual  = storage.entries(low, high, maxSize)
      assert(actual == expected)
    }
  }
}
