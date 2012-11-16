package com.wajam.spnl

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

@RunWith(classOf[JUnitRunner])
class TestTask extends FunSuite with BeforeAndAfter with MockitoSugar {
  var mockedFeed: Feeder = null
  var mockedAction: TaskAction = null
  var task: Task = null

  before {
    mockedFeed = mock[Feeder]
    mockedAction = mock[TaskAction]

    task = new Task(mockedFeed, mockedAction)
    task.start()
  }

  test("when a task is ticked, action is called when next value from feeder") {
    when(mockedFeed.next()).thenReturn(Some(Map("k" -> "val")))

    task.tick(sync = true)
    verify(mockedFeed).next()
    verify(mockedAction).call(same(task), anyObject(), anyInt())
  }

  test("when feeder returns no data or an exception, task should throttle") {
    var feedNext: () => Option[Map[String, Any]] = null
    when(mockedFeed.next()).then(new Answer[Option[Map[String, Any]]] {
      def answer(invocation: InvocationOnMock) = feedNext()
    })

    task.context.normalRate = 10
    task.context.throttleRate = 1

    // feeder returns data
    feedNext = () => Some(Map("k" -> "val"))
    task.tick(true)
    assert(!task.isThrottling)

    // feeder returns no data
    feedNext = () => None
    task.tick(true)
    assert(task.isThrottling)

    // feeder returns data
    feedNext = () => Some(Map("k" -> "val"))
    task.tick(true)
    assert(!task.isThrottling)

    // feeder throws exception
    feedNext = () => throw new Exception("testing")
    task.tick(true)
    assert(task.isThrottling)
  }
}
