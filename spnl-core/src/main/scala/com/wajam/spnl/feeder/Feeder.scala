package com.wajam.spnl.feeder

import com.wajam.spnl.{TaskData, TaskContext}
import scala.language.implicitConversions

/**
 * Task feeder which acts like an iterator that produces a map of data.
 */
trait Feeder {
  def name: String

  def init(context: TaskContext)

  def peek(): Option[TaskData]

  def next(): Option[TaskData]

  def ack(data: TaskData)

  def kill()
}

object Feeder {
  type FeederPredicate = TaskData => Boolean

  implicit def predicateToFeederFilter(predicate: FeederPredicate): FeederFilter = FeederFilter(predicate)
  implicit def feederFilterToPredicate(filter: FeederFilter): FeederPredicate = filter.predicate

  implicit def feederToFeederOps(feeder: Feeder): FeederOps = FeederOps(feeder)
}
