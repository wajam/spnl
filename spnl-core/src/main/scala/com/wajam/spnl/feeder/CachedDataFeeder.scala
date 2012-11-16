package com.wajam.spnl.feeder

import collection.immutable.Queue

/**
 * Feeder which uses a cache to store data
 * User: Alexandre Bergeron <alex@wajam.com>
 * Date: 16/11/12
 * Time: 2:25 PM
 */
abstract class CachedDataFeeder extends Feeder {

  var cache: Queue[Map[String, Any]] = Queue()

  def peek() = if (cache.isEmpty) {
    cache enqueue loadMore()
    None
  } else {
    Some(cache.head)
  }

  def next() = {
    if (cache.isEmpty) {
      cache enqueue loadMore()
      None
    } else {
      val (elem, rest) = cache.dequeue
      cache = rest
      Some(elem)
    }
  }

  def loadMore(): Iterable[Map[String, Any]]

}
