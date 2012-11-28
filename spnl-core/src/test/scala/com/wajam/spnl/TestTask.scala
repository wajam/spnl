package com.wajam.spnl

import feeder.Feeder
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
  var mockedAcceptor: TaskAcceptor = null
  var task: Task = null

  before {
    mockedFeed = mock[Feeder]
    mockedAction = mock[TaskAction]
    mockedAcceptor = mock[TaskAcceptor]
    when(mockedAcceptor.accept(anyObject())).thenReturn(true)

    task = new Task(mockedFeed, mockedAction, acceptor = mockedAcceptor)
    task.start()
  }

  test("when a task is ticked, action is called when next value from feeder") {
    when(mockedFeed.peek()).thenReturn(Some(Map("k" -> "val")))

    task.tick(sync = true)
    verify(mockedFeed).peek()
    verify(mockedFeed).next()
    verify(mockedAction).call(same(task), anyObject(), anyInt())
  }

  test("when feeder returns no data or an exception, task should throttle") {
    var feedNext: () => Option[Map[String, Any]] = null
    when(mockedFeed.peek()).then(new Answer[Option[Map[String, Any]]] {
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

  test("when feeder gives tokens, should not process two tasks with same token") {
    val data = Map("token" -> "asdf")
    val feedNext: () => Option[Map[String, Any]] = () => Some(data)
    when(mockedFeed.peek()).then(new Answer[Option[Map[String, Any]]] {
      def answer(invocation: InvocationOnMock) = feedNext()
    })

    task.context.normalRate = 10
    task.context.throttleRate = 1

    task.tick(true)
    assert(!task.isThrottling)

    task.tick(true)
    assert(task.isThrottling)

    task.tock(data)
    task.tick(true)
    assert(!task.isThrottling)
  }

  test("should call action only if accepted by acceptor") {
    val feedNext: () => Option[Map[String, Any]] = () => Some(Map("token" -> "0"))
    when(mockedFeed.peek()).then(new Answer[Option[Map[String, Any]]] {
      def answer(invocation: InvocationOnMock) = feedNext()
    })

    // Accept false
    reset(mockedAcceptor)
    when(mockedAcceptor.accept(anyObject())).thenReturn(false)
    task.tick(sync = true)
    verifyZeroInteractions(mockedAction)

    // Accept true
    reset(mockedAcceptor)
    when(mockedAcceptor.accept(anyObject())).thenReturn(true)
    task.tick(sync = true)
    verify(mockedAction).call(same(task), anyObject(), anyInt())
  }
}
