package com.wajam.spnl

/**
 * Task feeder which acts like an iterator that produces a map of data.
 */
trait Feeder {
  def init()

  def next(): Map[String, Any]

  def kill()
}
