package com.wajam.spnl.feeder

import com.wajam.spnl.TaskContext
import scala.annotation.tailrec

class FilteredFeeder(feeder: Feeder, filter: FeederFilter) extends Feeder {
  def name = feeder.name

  def init(context: TaskContext) {
    feeder.init(context)
  }

  def peek() = {
    @tailrec
    def filterPeek(): Option[Map[String, Any]] = {
      feeder.peek() match {
        case Some(d) if filter.predicate(d) => Some(d)
        case Some(d) => {
          feeder.next().map(feeder.ack(_))
          filterPeek()
        }
        case None => None
      }
    }
    filterPeek()
  }

  def next() = {
    @tailrec
    def filterNext(): Option[Map[String, Any]] = {
      feeder.next() match {
        case Some(d) if filter.predicate(d) => Some(d)
        case Some(d) => {
          feeder.ack(d)
          filterNext()
        }
        case None => None
      }
    }
    filterNext()
  }

  def ack(data: Map[String, Any]) {
    feeder.ack(data)
  }

  def kill() {
    feeder.kill()
  }
}

object FilteredFeeder {
  type FeederPredicate = Map[String,Any] => Boolean
  implicit def predicateToFeederFilter(predicate: FeederPredicate): FeederFilter = {
    FeederFilter(predicate)
  }

  implicit def feederToFeederOps(feeder: Feeder): FilterFeederUtil = {
    new FilterFeederUtil(feeder)
  }
}

class FilterFeederUtil(feeder: Feeder) {
  import FilteredFeeder._

  def withFilter(predicate: FilteredFeeder.FeederPredicate) = {
    new FilteredFeeder(feeder, predicate)
  }

  def withFilter(filter: FeederFilter) = {
    new FilteredFeeder(feeder, filter)
  }
}

case class FeederFilter(predicate: FilteredFeeder.FeederPredicate) {
  def or (otherPredicate: Map[String,Any] => Boolean) = {
    FeederFilter(data => (this.predicate(data) || otherPredicate(data)))
  }

  def and (otherPredicate: Map[String,Any] => Boolean) = {
    FeederFilter(data => (this.predicate(data) && otherPredicate(data)))
  }
}


