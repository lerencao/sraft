package org.nonsense.raft.storage

import org.nonsense.raft.Unstable
import org.nonsense.raft.protos.Protos.Entry

import scala.collection.mutable.ArrayBuffer

class UnstableTest extends UnitSpec with ProtoHelper {

  test("maybe first index") {
    val tests = List(
      (Some(newEntry(5, 1)), 5, None, None),
      (None, 0, None, None),
      // has snapshot
      (Some(newEntry(5, 1)), 5, Some(newSnapshot(4, 1)), Some(5)),
      (None, 5, Some(newSnapshot(4, 1)), Some(5))
    )
    for ((ents, offset, snapshot, windex) <- tests) {
      val u = Unstable(entries = ArrayBuffer(ents.toList: _*), offset = offset, snapshot = snapshot)
      assert(u.maybeFirstIndex() == windex)
    }
  }

  test("maybe last index") {
    val tests = List(
      (Some(newEntry(5, 1)), 5, None, Some(5)),
      (None, 0, None, None),
      // has snapshot
      (Some(newEntry(5, 1)), 5, Some(newSnapshot(4, 1)), Some(5)),
      (None, 5, Some(newSnapshot(4, 1)), Some(4))
    )
    for ((ents, offset, snapshot, windex) <- tests) {
      val u = Unstable(entries = ArrayBuffer(ents.toList: _*), offset = offset, snapshot = snapshot)
      assert(u.maybeLastIndex() == windex)
    }
  }

  test("maybe term") {
    val tests = List(
      // term from entries
      (Some(newEntry(5, 1)), 5, None, 5, true, 1),
      (Some(newEntry(5, 1)), 5, None, 6, false, 0),
      (Some(newEntry(5, 1)), 5, None, 4, false, 0),
      (
        Some(newEntry(5, 1)),
        5,
        Some(newSnapshot(4, 1)),
        5,
        true,
        1
      ),
      (
        Some(newEntry(5, 1)),
        5,
        Some(newSnapshot(4, 1)),
        6,
        false,
        0
      ),
      // term from snapshot
      (
        Some(newEntry(5, 1)),
        5,
        Some(newSnapshot(4, 1)),
        4,
        true,
        1
      ),
      (
        Some(newEntry(5, 1)),
        5,
        Some(newSnapshot(4, 1)),
        3,
        false,
        0
      ),
      (None, 5, Some(newSnapshot(4, 1)), 5, false, 0),
      (None, 5, Some(newSnapshot(4, 1)), 4, true, 1),
      (None, 0, None, 5, false, 0)
    )

    for ((entries, offset, snapshot, index, wok, wterm) <- tests) {
      val u = Unstable(entries = ArrayBuffer(entries.toList: _*), offset = offset, snapshot = snapshot)

      val term = u.maybeTerm(index)
      term match {
        case None => assert(!wok)
        case Some(t) => assert(t == wterm)
      }
    }
  }

  test("restore") {
    val u = Unstable(snapshot = Some(newSnapshot(4, 1)), offset = 5, entries = ArrayBuffer(newEntry(5, 1)))
    val s = newSnapshot(6, 2)
    u.restore(s)
    assert(u.entries.isEmpty)
    assert(u.offset == s.getMetadata.getIndex + 1)
    assert(u.snapshot.get == s)

  }

  test("Stable to") {
    val tests = List(
      (ArrayBuffer[Entry](), 0, None, 5, 1, 0, 0),
      // stable to the first entry
      (ArrayBuffer(newEntry(5, 1)), 5, None, 5, 1, 6, 0),
      (ArrayBuffer(newEntry(5, 1), newEntry(6, 0)), 5, None, 5, 1, 6, 1),
      // stable to the first entry and term mismatch
      (ArrayBuffer(newEntry(6, 2)), 5, None, 6, 1, 5, 1),
      // stable to old entry
      (ArrayBuffer(newEntry(5, 1)), 5, None, 4, 1, 5, 1),
      (ArrayBuffer(newEntry(5, 1)), 5, None, 4, 2, 5, 1),
      // with snapshot
      // stable to the first entry
      (
        ArrayBuffer(newEntry(5, 1)),
        5,
        Some(newSnapshot(4, 1)),
        5,
        1,
        6,
        0
      ),
      // stable to the first entry
      (
        ArrayBuffer(newEntry(5, 1), newEntry(6, 1)),
        5,
        Some(newSnapshot(4, 1)),
        5,
        1,
        6,
        1
      ),
      // stable to the first entry and term mismatch
      (
        ArrayBuffer(newEntry(6, 2)),
        5,
        Some(newSnapshot(5, 1)),
        6,
        1,
        5,
        1
      ),
      // stable to snapshot
      (
        ArrayBuffer(newEntry(5, 1)),
        5,
        Some(newSnapshot(4, 1)),
        4,
        1,
        5,
        1
      ),
      // stable to old entry
      (
        ArrayBuffer(newEntry(5, 2)),
        5,
        Some(newSnapshot(4, 2)),
        4,
        1,
        5,
        1
      )
    )

    for ((entries, offset, snapshot, index, term, woffset, wlen) <- tests) {
      val u = Unstable(
        entries = entries,
        offset = offset,
        snapshot = snapshot
      )

      u.stableTo(index, term)
      assert(u.offset == woffset)
      assert(u.entries.length == wlen)
    }
  }


  test("Truncate and append") {
    val tests = List(
      // replace to the end
      (
        ArrayBuffer(newEntry(5, 1)),
        5,
        None,
        ArrayBuffer(newEntry(6, 1), newEntry(7, 1)),
        5,
        ArrayBuffer(newEntry(5, 1), newEntry(6, 1), newEntry(7, 1))
      ),

      // replace to unstable entries
      (
        ArrayBuffer(newEntry(5, 1)),
        5,
        None,
        ArrayBuffer(newEntry(5, 2), newEntry(6, 2)),
        5,
        ArrayBuffer(newEntry(5, 2), newEntry(6, 2))
      ),

      (
        ArrayBuffer(newEntry(5, 1)),
        5,
        None,
        ArrayBuffer(newEntry(4, 2), newEntry(5, 2), newEntry(6, 2)),
        4,
        ArrayBuffer(newEntry(4, 2), newEntry(5, 2), newEntry(6, 2))
      ),

      // truncate existing entries and append
      (
        ArrayBuffer(newEntry(5, 1), newEntry(6, 1), newEntry(7, 1)),
        5,
        None,
        ArrayBuffer(newEntry(6, 2)),
        5,
        ArrayBuffer(newEntry(5, 1), newEntry(6, 2))
      ),

      (
        ArrayBuffer(newEntry(5, 1), newEntry(6, 1), newEntry(7, 1)),
        5,
        None,
        ArrayBuffer(newEntry(7, 2), newEntry(8, 2)),
        5,
        ArrayBuffer(newEntry(5, 1), newEntry(6, 1), newEntry(7, 2), newEntry(8, 2))
      )
    )

    for ((entries, offset, snapshot, to_append, woffset, wentries) <- tests) {
      val u = Unstable(
        entries = entries,
        offset = offset,
        snapshot = snapshot
      )
      u.truncateAndAppend(to_append)
      assert(u.offset == woffset)
      assert(u.entries.size == wentries.size)
      if (u.entries.nonEmpty) {
        assert(u.entries.head.getIndex == wentries.head.getIndex)
        assert(u.entries.head.getTerm == wentries.head.getTerm)
      }
    }
  }
}
