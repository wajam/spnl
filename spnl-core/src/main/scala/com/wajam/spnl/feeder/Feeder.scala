package com.wajam.spnl.feeder

import com.wajam.spnl.TaskContext

/**
 * Task feeder which acts like an iterator that produces a map of data.
 */
trait Feeder {
  def name: String

  def init(context: TaskContext)

  def peek(): Option[Map[String, Any]]

  def next(): Option[Map[String, Any]]

  def ack(data: Map[String, Any])

  def kill()
}
