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

    Thread.sleep(300)
    verify(mockedTask, times(1)).tick()
  }

  test("rate less than 1 per second") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]

    when(mockedTask.currentRate).thenReturn(0.5)
    scheduler.startTask(mockedTask)

    Thread.sleep(1300)
    verify(mockedTask, times(1)).tick()
  }

  test("miliseconds to micro switch") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]

    var rate = 100
    when(mockedTask.currentRate).then(new Answer[Int] {
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
    when(mockedTask.currentRate).then(new Answer[Int] {
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

    var stoped = false
    var tickAfterStop = 0
    when(mockedTask.currentRate).thenReturn(10)
    when(mockedTask.tick(anyBoolean())).then(new Answer[Unit] {
      def answer(invocation: InvocationOnMock) {
        if (stoped) {
          tickAfterStop += 1
        }
      }
    })

    scheduler.startTask(mockedTask)
    Thread.sleep(200)
    scheduler.endTask(mockedTask)
    stoped = true
    Thread.sleep(500)
    tickAfterStop should be(0)
  }
}
