package com.wajam.spnl.feeder

import collection.immutable.Queue
import com.yammer.metrics.scala.Instrumented

/**
 * Feeder which uses a cache to store data
 * User: Alexandre Bergeron <alex@wajam.com>
 * Date: 16/11/12
 * Time: 2:25 PM
 */
abstract class CachedDataFeeder(val name: String) extends Feeder with Instrumented {

  lazy val loadMoreTimer = metrics.timer("load-more-time", name)
  lazy val loadMoreMeter = metrics.meter("load-more-calls", "load-more-calls", name)
  lazy val loadMoreRecords = metrics.meter("load-more-records", "load-more-records", name)

  var cache: Queue[Map[String, Any]] = Queue()

  def peek() = {
    if (cache.isEmpty) {
      loadCache()
      None
    } else {
      Some(cache.head)
    }
  }

  def next() = {
    if (cache.isEmpty) {
      loadCache()
      None
    } else {
      val (elem, rest) = cache.dequeue
      cache = rest
      Some(elem)
    }
  }

  def loadMore(): Iterable[Map[String, Any]]

  private def loadCache() {
    loadMoreMeter.mark()

    val records = loadMoreTimer.time {
      loadMore()
    }

    val before = cache.size
    cache ++= records
    loadMoreRecords.mark(cache.size - before)
  }

}
