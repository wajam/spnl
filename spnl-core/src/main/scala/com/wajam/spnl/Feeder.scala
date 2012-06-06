package com.wajam.spnl

/**
 * Task feeder which acts like an iterator that produces a map of data.
 */
trait Feeder {
  def init(context: TaskContext)

  def next(): Option[Map[String, Any]]

  def kill()
}
