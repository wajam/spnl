package com.wajam.spnl

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.matchers.ShouldMatchers._
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

@RunWith(classOf[JUnitRunner])
class TestScheduler extends FunSuite with MockitoSugar {

  test("miliseconds") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]

    when(mockedTask.currentRate).thenReturn(1000)
    scheduler.startTask(mockedTask)

    Thread.sleep(300)
    verify(mockedTask, atLeast(100)).tick()
  }

  test("seconds") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]

    when(mockedTask.currentRate).thenReturn(1)
    scheduler.startTask(mockedTask)

    Thread.sleep(1000)
    verify(mockedTask, times(1)).tick()
  }

  test("10 seconds") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]

    when(mockedTask.currentRate).thenReturn(0.1)
    scheduler.startTask(mockedTask)

    Thread.sleep(10000)
    verify(mockedTask, times(1)).tick()
  }

  test("rate less than 1 per second") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]

    when(mockedTask.currentRate).thenReturn(0.5)
    scheduler.startTask(mockedTask)

    Thread.sleep(2000)
    verify(mockedTask, times(1)).tick()
  }

  test("miliseconds to micro switch") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]

    var rate = 100
    when(mockedTask.currentRate).thenAnswer(new Answer[Int] {
      def answer(invocation: InvocationOnMock) = rate
    })

    scheduler.startTask(mockedTask)
    Thread.sleep(300)

    rate = 10000
    Thread.sleep(1000)

    verify(mockedTask, atLeast(2000)).tick()
  }

  test("Same task added") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]

    var rate = 100
    when(mockedTask.currentRate).thenAnswer(new Answer[Int] {
      def answer(invocation: InvocationOnMock) = rate
    })

    scheduler.startTask(mockedTask)
    scheduler.startTask(mockedTask)

    assert(scheduler.tasks.size === 1)
    Thread.sleep(200)

    scheduler.endTask(mockedTask)

    assert(scheduler.tasks.size === 0)
  }

  test("end task should not call task anymore") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]

    var stopped = false
    var tickAfterStop = 0
    when(mockedTask.currentRate).thenReturn(10)
    when(mockedTask.tick(anyBoolean())).thenAnswer(new Answer[Unit] {
      def answer(invocation: InvocationOnMock) {
        if (stopped) {
          tickAfterStop += 1
        }
      }
    })

    scheduler.startTask(mockedTask)
    Thread.sleep(200)
    scheduler.endTask(mockedTask)
    stopped = true
    Thread.sleep(500)
    tickAfterStop should be <= 1
  }

  test("can stop, and restart with a task according to the rate") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]
    val mockedContext = mock[TaskContext]

    when(mockedTask.context).thenReturn(mockedContext)

    var stopped = false
    var tickAfterStop = 0
    var tick = 0

    when(mockedTask.tick(anyBoolean())).thenAnswer(new Answer[Unit] {
      def answer(invocation: InvocationOnMock) {
        if (stopped) {
          tickAfterStop += 1
        }

        tick += 1
      }
    })

    val rate = 100

    when(mockedContext.normalRate).thenReturn(rate)
    when(mockedTask.currentRate).thenReturn(rate)

    // Start at rate 100
    scheduler.startTask(mockedTask)

    // Check if it ticked
    Thread.sleep(500)
    tick should be >= 10

    // Stop with rate
    when(mockedContext.normalRate).thenReturn(0)
    when(mockedTask.currentRate).thenReturn(0)

    // Wait to sure we don't pick rogue tick
    Thread.sleep(500)
    stopped = true
    val firstRunTick = tick

    Thread.sleep(500)
    tickAfterStop should be(0)

    // Restart
    when(mockedContext.normalRate).thenReturn(rate)
    when(mockedTask.currentRate).thenReturn(rate)

    Thread.sleep(500)

    // Check if it ticked
    val secondRunTick = tick - firstRunTick
    secondRunTick should be >= 10
  }
}
