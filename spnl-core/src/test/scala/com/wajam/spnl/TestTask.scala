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
import com.wajam.nrv.UnavailableException
import com.wajam.commons.{IdGenerator, ControlableCurrentTime}
import com.wajam.spnl.feeder.Feeder.FeederData

@RunWith(classOf[JUnitRunner])
class TestTask extends FunSuite with BeforeAndAfter with MockitoSugar {
  var mockedFeed: Feeder = null
  var mockedAction: TaskAction = null
  var taskContext: TaskContext = null
  var task: Task with ControlableCurrentTime = null

  val feederData = Map("token" -> 1L, "k" -> "val")
  var taskData: TaskData = _

  implicit var idGenerator: DummyIdGenerator = _

  class DummyIdGenerator extends IdGenerator[Long] {
    var current = 1L

    def nextId = {
      current
    }

    def increment(): Unit = {
      current = current + 1
    }
  }

  before {
    idGenerator = new DummyIdGenerator

    taskData = TaskData(feederData)

    taskContext = new TaskContext(normalRate = 10, throttleRate = 1, maxConcurrent = 5)
    mockedFeed = mock[Feeder]
    mockedAction = mock[TaskAction]
    when(mockedFeed.name).thenReturn("test_task")

    task = new Task(mockedFeed, mockedAction, context = taskContext)(idGenerator)
      with ControlableCurrentTime
    task.start()
    reset(mockedFeed) // Reset interaction recorded during start
  }

  test("when a task is ticked, action is called when next value from feeder") {
    when(mockedFeed.peek()).thenReturn(Some(feederData))

    task.tick(sync = true)
    verify(mockedFeed).peek()
    verify(mockedFeed).next()
    verify(mockedAction).call(same(task), anyObject())
  }

  test("task kill") {
    when(mockedFeed.peek()).thenReturn(Some(feederData))

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
    var feedNext: () => Option[FeederData] = null
    when(mockedFeed.peek()).thenAnswer(new Answer[Option[FeederData]] {
      def answer(invocation: InvocationOnMock) = feedNext()
    })

    task.context.normalRate should be > task.context.throttleRate

    // feeder returns data
    feedNext = () => Some(feederData)
    task.tick(sync = true)
    assert(!task.isThrottling)

    // feeder returns no data
    feedNext = () => None
    task.tick(sync = true)
    assert(task.isThrottling)

    // feeder returns data
    feedNext = () => Some(feederData + ("token" -> 2L))
    task.tick(sync = true)
    assert(!task.isThrottling)

    // feeder throws exception
    feedNext = () => throw new Exception("testing")
    task.tick(sync = true)
    assert(task.isThrottling)
  }

  test("should not process more than max concurrent process") {
    var data = feederData
    val feedNext: () => Option[FeederData] = () => Some(data)
    when(mockedFeed.peek()).thenAnswer(new Answer[Option[FeederData]] {
      def answer(invocation: InvocationOnMock) = feedNext()
    })

    task.context.maxConcurrent = 2
    var concurrentCounter = new MetricsGroup(task.getClass).counter("concurrent-count", task.name)
    concurrentCounter.clear()

    // Verify before
    concurrentCounter.count should be(0)
    verifyZeroInteractions(mockedAction)

    // First tick
    data = feederData + ("token" -> 1L)
    task.tick(sync = true)
    concurrentCounter.count should be(1)
    verify(mockedFeed, times(1)).peek()
    verify(mockedFeed, times(1)).next()
    verify(mockedAction, times(1)).call(same(task), anyObject())

    // Second tick
    data = feederData + ("token" -> 2L)
    task.tick(sync = true)
    concurrentCounter.count should be(2)
    verify(mockedFeed, times(2)).peek()
    verify(mockedFeed, times(2)).next()
    verify(mockedAction, times(2)).call(same(task), anyObject())

    // No more concurrent process untill tock
    data = feederData + ("token" -> 3L)
    task.tick(sync = true)
    data = feederData + ("token" -> 4L)
    task.tick(sync = true)
    concurrentCounter.count should be(2)
    verifyNoMoreInteractions(mockedFeed)
    verify(mockedAction, times(2)).call(same(task), anyObject())

    task.tock(TaskData(1, 0))
    task.tock(TaskData(2, 0))
    data = feederData + ("token" -> 5L)
    task.tick(sync = true)
    concurrentCounter.count should be(1)
    verify(mockedFeed, times(2)).ack(anyObject())
    verify(mockedFeed, times(3)).peek()
    verify(mockedFeed, times(3)).next()
    verify(mockedAction, times(3)).call(same(task), anyObject())

    data = feederData + ("token" -> 6L)
    task.tick(sync = true)
    concurrentCounter.count should be(2)
    verify(mockedFeed, times(4)).peek()
    verify(mockedFeed, times(4)).next()
    verify(mockedAction, times(4)).call(same(task), anyObject())

    data = feederData + ("token" -> 7L)
    task.tick(sync = true)
    concurrentCounter.count should be(2)
    verifyNoMoreInteractions(mockedFeed)
    verify(mockedAction, times(4)).call(same(task), anyObject())
  }

  def throttleTest(exceptionGenerator: () => Exception, WaitTimeGenerator: Int => Long) {
    when(mockedFeed.peek()).thenReturn(Some(feederData))

    // These metrics track the number of retry attempts (attempts that throw exceptions).
    // Setup counters initial value. They will be validated at the end to ensure they are reset to that value
    // and not to zero.
    val retryCounter = new MetricsGroup(task.getClass).counter("retry-count", task.name)
    val globalCounter = new MetricsGroup(task.getClass).counter("retry-count")
    val initialOffset = retryCounter.count
    val initialGlobalOffset = globalCounter.count

    retryCounter += 50
    globalCounter += 100
    retryCounter.count should be(50 + initialOffset)
    globalCounter.count should be(100 + initialGlobalOffset)

    task.currentRate should be(taskContext.normalRate)

    task.tick(sync = true)
    verify(mockedFeed, times(1)).peek()
    verify(mockedFeed, times(1)).next()

    // Failures
    val failCount = 5
    for (i <- 1 to failCount) {
      task.fail(taskData, exceptionGenerator())
      task.tick(sync = true)

      // Tick without advancing time, should not retry
      task.currentRate should be(taskContext.throttleRate)
      verify(mockedFeed, times(i * 2)).peek()
      verify(mockedFeed, times(1)).next()

      for (j <- 1 to i)
        verify(mockedAction).call(task, taskData.copy(retryCount = j - 1))

      task.advanceTime(WaitTimeGenerator(i)) // consider worst random case

      // Tick after advancing time, should retry
      task.tick(sync = true)
      verify(mockedFeed, times(1 + i * 2)).peek()
      verify(mockedFeed, times(1)).next()

      for (j <- 1 to i)
        verify(mockedAction).call(task, taskData.copy(retryCount = j))

      retryCounter.count should be(50 + initialOffset + i)
      globalCounter.count should be(100 + initialGlobalOffset + i)
    }

    // Success!!!
    task.tock(taskData)
    task.tick(sync = true)
    task.currentRate should be (taskContext.normalRate)
    retryCounter.count should be(50 + initialOffset)
    globalCounter.count should be(100 + initialGlobalOffset)
    verify(mockedFeed, times(2 + failCount * 2)).peek()
    verify(mockedFeed, times(2)).next()
    verify(mockedAction).call(task, taskData.copy(retryCount = failCount))
  }

  test("Should throttle and retry when generic errors occur") {
    throttleTest(() => {new Exception}, attempt => {((math.pow(2, attempt).toLong + 1.5) * 1000).toLong})
  }

  //this test is identical to the previous one, but it uses a different exception to trigger the alternative delay function
  test("Should throttle in a much slower way when when UnavailableException occur") {
    throttleTest(() => {new UnavailableException}, attempt => {5000 + ((5 + math.pow(2, attempt).toLong) * 1000)})
  }

  test("Should not allow concurrent tasks for same token by default") {
    when(mockedFeed.peek()).thenReturn(Some(feederData))

    task.tick(sync = true)
    verify(mockedAction, times(1)).call(same(task), anyObject())

    idGenerator.increment()

    task.tick(sync = true)
    verify(mockedAction, times(1)).call(same(task), anyObject())

    task.tock(taskData)

    idGenerator.increment()

    task.tick(sync = true)
    verify(mockedAction, times(2)).call(same(task), anyObject())
  }

  test("Should allow concurrent tasks for same token if allowSameTokenConcurrency is set to true") {
    taskContext = taskContext.copy(allowSameTokenConcurrency = true)

    task = new Task(mockedFeed, mockedAction, context = taskContext)(idGenerator)
      with ControlableCurrentTime
    task.start()

    when(mockedFeed.peek()).thenReturn(Some(feederData))

    task.tick(sync = true)
    verify(mockedAction, times(1)).call(same(task), anyObject())

    idGenerator.increment()

    task.tick(sync = true)
    verify(mockedAction, times(2)).call(same(task), anyObject())

    task.tock(taskData)

    idGenerator.increment()

    task.tick(sync = true)
    verify(mockedAction, times(3)).call(same(task), anyObject())
  }
}
