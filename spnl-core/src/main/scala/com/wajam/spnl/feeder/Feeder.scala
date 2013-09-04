package com.wajam.spnl.feeder

import com.wajam.spnl.TaskContext
import scala.language.implicitConversions

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

object Feeder {
  type FeederPredicate = Map[String,Any] => Boolean

  implicit def predicateToFeederFilter(predicate: FeederPredicate): FeederFilter = FeederFilter(predicate)
  implicit def feederFilterToPredicate(filter: FeederFilter): FeederPredicate = filter.predicate

  implicit def feederToFeederOps(feeder: Feeder): FeederOps = FeederOps(feeder)
}
