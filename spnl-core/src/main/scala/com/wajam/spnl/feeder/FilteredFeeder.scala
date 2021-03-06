package com.wajam.spnl.feeder

import com.wajam.spnl.{TaskData, TaskContext}
import scala.annotation.tailrec
import com.wajam.spnl.feeder.Feeder.FeederData

/**
 * Wraps an existing feeder dans implement filtering of the feeder's data based on a predicate.
 *
 * @param feeder The existing feeder
 * @param filter The filtering function
 */
class FilteredFeeder(feeder: Feeder, filter: Feeder.FeederPredicate) extends Feeder {
  def name = feeder.name

  def init(context: TaskContext) {
    feeder.init(context)
  }

  def peek() = {
    @tailrec
    def filterPeek(): Option[FeederData] = {
      feeder.peek() match {
        case Some(d) if filter(d) => Some(d)
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
    def filterNext(): Option[FeederData] = {
      feeder.next() match {
        case Some(d) if filter(d) => Some(d)
        case Some(d) => {
          feeder.ack(d)
          filterNext()
        }
        case None => None
      }
    }
    filterNext()
  }

  def ack(data: FeederData) {
    feeder.ack(data)
  }

  def kill() {
    feeder.kill()
  }
}

