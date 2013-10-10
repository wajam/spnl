package com.wajam.spnl.feeder

import com.wajam.spnl.TaskContext
import scala.language.implicitConversions
import com.wajam.spnl.feeder.Feeder.FeederData

/**
 * Task feeder which acts like an iterator that produces a map of data.
 */
trait Feeder {
  def name: String

  def init(context: TaskContext)

  def peek(): Option[FeederData]

  def next(): Option[FeederData]

  def ack(data: FeederData)

  def kill()
}

object Feeder {
  type FeederData = Map[String, Any]
  type FeederPredicate = FeederData => Boolean

  implicit def predicateToFeederFilter(predicate: FeederPredicate): FeederFilter = FeederFilter(predicate)
  implicit def feederFilterToPredicate(filter: FeederFilter): FeederPredicate = filter.predicate

  implicit def feederToFeederOps(feeder: Feeder): FeederOps = FeederOps(feeder)
}
