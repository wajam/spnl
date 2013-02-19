package com.wajam.spnl.feeder

import com.wajam.spnl.TaskContext
import com.wajam.nrv.data.MValue

/**
 * Task feeder which acts like an iterator that produces a map of data.
 */
trait Feeder {
  def name: String

  def init(context: TaskContext)

  def peek(): Option[Map[String, MValue]]

  def next(): Option[Map[String, MValue]]

  def ack(data: Map[String, MValue])

  def kill()
}

