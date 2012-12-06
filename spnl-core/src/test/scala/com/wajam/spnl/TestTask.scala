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
import com.yammer.metrics.scala.MetricsGroup
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.utils.ControlableCurrentTime

@RunWith(classOf[JUnitRunner])
class TestTask extends FunSuite with BeforeAndAfter with MockitoSugar {
  var mockedFeed: Feeder = null
  var mockedAction: TaskAction = null
  var mockedAcceptor: TaskAcceptor = null
  var taskContext: TaskContext = null
  var task: Task with ControlableCurrentTime = null

  before {
    taskContext = new TaskContext(normalRate = 10, throttleRate = 1)
    mockedFeed = mock[Feeder]
    mockedAction = mock[TaskAction]
    mockedAcceptor = mock[TaskAcceptor]
    when(mockedAcceptor.accept(anyObject())).thenReturn(true)

    task = new Task("test_task", mockedFeed, mockedAction, context = taskContext, acceptor = mockedAcceptor)
      with ControlableCurrentTime
    task.start()
  }

  test("when a task is ticked, action is called when next value from feeder") {
    when(mockedFeed.peek()).thenReturn(Some(Map("k" -> "val")))

    task.tick(sync = true)
    verify(mockedFeed).peek()
    verify(mockedFeed).next()
    verify(mockedAction).call(same(task), anyObject())
  }

  test("when feeder returns no data or an exception, task should throttle") {
    var feedNext: () => Option[Map[String, Any]] = null
    when(mockedFeed.peek()).then(new Answer[Option[Map[String, Any]]] {
      def answer(invocation: InvocationOnMock) = feedNext()
    })

    task.context.normalRate should be > task.context.throttleRate

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

    task.context.normalRate should be > task.context.throttleRate

    task.tick(true)
    assert(!task.isThrottling)

    task.tick(true)
    assert(task.isThrottling)

    task.tock(data)
    task.tick(true)
    assert(!task.isThrottling)
  }

  test("should call action only if accepted by acceptor") {
    var index = 0
    val feedPeek: () => Option[Map[String, Any]] = () => Some(Map("token" -> index.toString))
    val feedNext: () => Option[Map[String, Any]] = () => Some(Map("token" -> {
      val value = index.toString
      index += 1
      value
    }))
    when(mockedFeed.peek()).then(new Answer[Option[Map[String, Any]]] {
      def answer(invocation: InvocationOnMock) = feedPeek()
    })
    when(mockedFeed.next()).then(new Answer[Option[Map[String, Any]]] {
      def answer(invocation: InvocationOnMock) = feedNext()
    })

    // Accept false
    reset(mockedAcceptor)
    when(mockedAcceptor.accept(anyObject())).thenReturn(false)
    task.tick(sync = true)
    verifyZeroInteractions(mockedAction)
    index should be(1)

    // Accept false again
    reset(mockedAcceptor)
    when(mockedAcceptor.accept(anyObject())).thenReturn(false)
    task.tick(sync = true)
    verifyZeroInteractions(mockedAction)
    index should be(2)

    // Accept true
    reset(mockedAcceptor)
    when(mockedAcceptor.accept(anyObject())).thenReturn(true)
    task.tick(sync = true)
    verify(mockedAction).call(same(task), anyObject())
    index should be(3)
  }

  test("Should throttle and retry on errors") {
    val data = Map("k" -> "val")
    reset(mockedFeed) // Reset interaction recorded during start
    when(mockedFeed.peek()).thenReturn(Some(data))

    // Setup counters initial value. They will be validated at the end to ensure they are reset to that value
    // and not to zero
    var retryCounter = new MetricsGroup(task.getClass).counter("retry", task.name)
    var globalCounter = new MetricsGroup(task.getClass).counter("retry")
    retryCounter += 50
    globalCounter += 100
    retryCounter.count should be(50)
    globalCounter.count should be(100)

    task.currentRate should be(taskContext.normalRate)

    // Failures
    val failCount = 5
    for (i <- 1 to failCount) {
      task.fail(data, new Exception)
      task.tick(sync = true)

      task.currentRate should be(taskContext.throttleRate)
      verifyZeroInteractions(mockedFeed)
      verify(mockedAction, times(i - 1)).call(same(task), same(data))
      task.advanceTime(math.pow(2, i).toLong * 1000 + 1000)

      task.tick(sync = true)
      verifyZeroInteractions(mockedFeed)
      verify(mockedAction, times(i)).call(same(task), same(data))
      retryCounter.count should be(50 + i)
      globalCounter.count should be(100 + i)
    }

    // Success!!!
    task.tock(data)
    task.tick(sync = true)
    task.currentRate should be (taskContext.normalRate)
    retryCounter.count should be(50)
    globalCounter.count should be(100)
    verify(mockedFeed).peek()
    verify(mockedFeed).next()
    verify(mockedAction, times(failCount + 1)).call(same(task), same(data))
  }
}
