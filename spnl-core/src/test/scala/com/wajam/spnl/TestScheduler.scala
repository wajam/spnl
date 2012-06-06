package com.wajam.spnl

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

@RunWith(classOf[JUnitRunner])
class TestScheduler extends FunSuite with MockitoSugar {

  test("miliseconds") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]

    when(mockedTask.rate).thenReturn(1000)
    scheduler.startTask(mockedTask)

    Thread.sleep(200)
    verify(mockedTask, atLeast(100)).tick()
  }

  test("seconds") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]

    when(mockedTask.rate).thenReturn(1)
    scheduler.startTask(mockedTask)

    Thread.sleep(200)
    verify(mockedTask, times(1)).tick()
  }

  test("miliseconds to micro switch") {
    val scheduler = new Scheduler
    val mockedTask = mock[Task]

    var rate = 100
    when(mockedTask.rate).then(new Answer[Int] {
      def answer(invocation: InvocationOnMock) = rate
    })

    scheduler.startTask(mockedTask)
    Thread.sleep(200)

    rate = 10000
    Thread.sleep(700)

    verify(mockedTask, atLeast(5000)).tick()
  }
}
