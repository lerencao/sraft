package org.nonsense.raft.utils

sealed trait Result[T, E] {
  def isOk: Boolean
  def isErr: Boolean = !isOk

  def ok: Option[T] = this match {
    case Ok(v) => Some(v)
    case _     => None
  }

  def err: Option[E] = this match {
    case Ok(_)  => None
    case Err(e) => Some(e)
  }

  def get: T = this.unwrap("call get on an `Err` value")

  def getOrElse(op: => T): T = this match {
    case Ok(v)  => v
    case Err(e) => op
  }

  def unwrap(msg: String): T = this match {
    case Ok(v)  => v
    case Err(e) => throw new Exception(s"$msg: $e")
  }
}

case class Ok[T, E](v: T) extends Result[T, E] {
  val isOk: Boolean = true
}

case class Err[T, E](e: E) extends Result[T, E] {
  val isOk: Boolean = false
}
