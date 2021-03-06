package com.wajam.spnl.feeder

import com.wajam.spnl.{TaskData, TaskContext}
import org.scalatest.FunSuite
import com.wajam.spnl.feeder.Feeder.FeederData

/**
 * User: Alexandre Bergeron <alex@wajam.com>
 * Date: 20/11/12
 * Time: 2:43 PM
 */
class TestCachedDataFeeder extends FunSuite {

  val elem1 = Map("token" -> 0L, "a" -> "b")
  val elem2 = Map("token" -> 0L, "a" -> "c")

  class CachedDataFeederImpl extends CachedDataFeeder {
    val name = "test_name"
    var elems = List(elem1, elem2)
    def loadMore() = elems match {
      case x :: xs => {
        elems = xs
        Iterable(x)
      }
      case Nil => Nil
    }

    def init(context: TaskContext) {}

    def kill() {}

    def ack(data: FeederData) {}
  }

  test("should peek and access next element") {
    val feeder = new CachedDataFeederImpl

    assert(feeder.peek() == None)
    assert(feeder.peek() === Some(elem1))
    assert(feeder.next() === Some(elem1))
    assert(feeder.next() === None)
    assert(feeder.next() == Some(elem2))
    assert(feeder.next() === None)
    assert(feeder.peek() === None)
    assert(feeder.next() === None)
  }

}
