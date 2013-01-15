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
  var taskContext: TaskContext = null
  var task: Task with ControlableCurrentTime = null

  before {
    taskContext = new TaskContext(normalRate = 10, throttleRate = 1, maxConcurrent = 5)
    mockedFeed = mock[Feeder]
    mockedAction = mock[TaskAction]

    task = new Task("test_task", mockedFeed, mockedAction, context = taskContext)
      with ControlableCurrentTime
    task.start()
    reset(mockedFeed) // Reset interaction recorded during start
  }

  test("when a task is ticked, action is called when next value from feeder") {
    when(mockedFeed.peek()).thenReturn(Some(Map("k" -> "val")))

    task.tick(sync = true)
    verify(mockedFeed).peek()
    verify(mockedFeed).next()
    verify(mockedAction).call(same(task), anyObject())
  }

  test("task kill") {
    when(mockedFeed.peek()).thenReturn(Some(Map("k" -> "val")))

    task.kill()
    task.tick(sync = false)
    task.tick(sync = false)
    task.tick(sync = false)
    task.tick(sync = false)
    Thread.sleep(300)
    verify(mockedFeed).kill()
    verifyNoMoreInteractions(mockedFeed)
    verifyZeroInteractions(mockedAction)
  }

  test("when feeder returns no data or an exception, task should throttle") {
    var feedNext: () => Option[Map[String, Any]] = null
    when(mockedFeed.peek()).then(new Answer[Option[Map[String, Any]]] {
      def answer(invocation: InvocationOnMock) = feedNext()
    })

    task.context.normalRate should be > task.context.throttleRate

    // feeder returns data
    feedNext = () => Some(Map("token" -> "0"))
    task.tick(sync = true)
    assert(!task.isThrottling)

    // feeder returns no data
    feedNext = () => None
    task.tick(sync = true)
    assert(task.isThrottling)

    // feeder returns data
    feedNext = () => Some(Map("token" -> "1"))
    task.tick(sync = true)
    assert(!task.isThrottling)

    // feeder throws exception
    feedNext = () => throw new Exception("testing")
    task.tick(sync = true)
    assert(task.isThrottling)
  }

  test("when feeder gives tokens, should not process two tasks with same token") {
    val data = Map("token" -> "asdf")
    val feedNext: () => Option[Map[String, Any]] = () => Some(data)
    when(mockedFeed.peek()).then(new Answer[Option[Map[String, Any]]] {
      def answer(invocation: InvocationOnMock) = feedNext()
    })

    task.context.normalRate should be > task.context.throttleRate

    task.tick(sync = true)
    verify(mockedAction, times(1)).call(same(task), anyObject())

    task.tick(sync = true)
    verify(mockedAction, times(1)).call(same(task), anyObject())

    task.tock(data)
    task.tick(sync = true)
    verify(mockedAction, times(2)).call(same(task), anyObject())
  }

  test("should not process more than max concurrent process") {
    var data = Map("token" -> "0")
    val feedNext: () => Option[Map[String, Any]] = () => Some(data)
    when(mockedFeed.peek()).then(new Answer[Option[Map[String, Any]]] {
      def answer(invocation: InvocationOnMock) = feedNext()
    })

    task.context.maxConcurrent = 2
    var concurrentCounter = new MetricsGroup(task.getClass).counter("concurrent-count", task.name)
    concurrentCounter.clear()

    // Verify before
    concurrentCounter.count should be(0)
    verifyZeroInteractions(mockedAction)

    // First tick
    data = Map("token" -> "1")
    task.tick(sync = true)
    concurrentCounter.count should be(1)
    verify(mockedFeed, times(1)).peek()
    verify(mockedFeed, times(1)).next()
    verify(mockedAction, times(1)).call(same(task), anyObject())

    // Second tick
    data = Map("token" -> "2")
    task.tick(sync = true)
    concurrentCounter.count should be(2)
    verify(mockedFeed, times(2)).peek()
    verify(mockedFeed, times(2)).next()
    verify(mockedAction, times(2)).call(same(task), anyObject())

    // No more concurrent process untill tock
    data = Map("token" -> "3")
    task.tick(sync = true)
    data = Map("token" -> "4")
    task.tick(sync = true)
    concurrentCounter.count should be(2)
    verifyNoMoreInteractions(mockedFeed)
    verify(mockedAction, times(2)).call(same(task), anyObject())

    task.tock(Map("token" -> "1"))
    task.tock(Map("token" -> "2"))
    data = Map("token" -> "5")
    task.tick(sync = true)
    concurrentCounter.count should be(1)
    verify(mockedFeed, times(2)).ack(anyObject())
    verify(mockedFeed, times(3)).peek()
    verify(mockedFeed, times(3)).next()
    verify(mockedAction, times(3)).call(same(task), anyObject())

    data = Map("token" -> "6")
    task.tick(sync = true)
    concurrentCounter.count should be(2)
    verify(mockedFeed, times(4)).peek()
    verify(mockedFeed, times(4)).next()
    verify(mockedAction, times(4)).call(same(task), anyObject())

    data = Map("token" -> "7")
    task.tick(sync = true)
    concurrentCounter.count should be(2)
    verifyNoMoreInteractions(mockedFeed)
    verify(mockedAction, times(4)).call(same(task), anyObject())
  }

  test("Should throttle and retry on errors") {
    val data = Map("token" -> "0")
    when(mockedFeed.peek()).thenReturn(Some(data))

    // Setup counters initial value. They will be validated at the end to ensure they are reset to that value
    // and not to zero
    var retryCounter = new MetricsGroup(task.getClass).counter("retry-count", task.name)
    var globalCounter = new MetricsGroup(task.getClass).counter("retry-count")
    retryCounter += 50
    globalCounter += 100
    retryCounter.count should be(50)
    globalCounter.count should be(100)

    task.currentRate should be(taskContext.normalRate)

    task.tick(sync = true)
    verify(mockedFeed, times(1)).peek()
    verify(mockedFeed, times(1)).next()

    // Failures
    val failCount = 5
    for (i <- 1 to failCount) {
      task.fail(data, new Exception)
      task.tick(sync = true)

      // Tick without advancing time, should not retry
      task.currentRate should be(taskContext.throttleRate)
      verify(mockedFeed, times(i * 2)).peek()
      verify(mockedFeed, times(1)).next()
      verify(mockedAction, times(i)).call(same(task), same(data))
      task.advanceTime(math.pow(2, i).toLong * 1000 + 1000)

      // Tick after advancing time, should retry
      task.tick(sync = true)
      verify(mockedFeed, times(1 + i * 2)).peek()
      verify(mockedFeed, times(1)).next()
      verify(mockedAction, times(i + 1)).call(same(task), same(data))
      retryCounter.count should be(50 + i)
      globalCounter.count should be(100 + i)
    }

    // Success!!!
    task.tock(data)
    task.tick(sync = true)
    task.currentRate should be (taskContext.normalRate)
    retryCounter.count should be(50)
    globalCounter.count should be(100)
    verify(mockedFeed, times(2 + failCount * 2)).peek()
    verify(mockedFeed, times(2)).next()
    verify(mockedAction, times(failCount + 2)).call(same(task), same(data))
  }
}
