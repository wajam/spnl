package com.wajam.spnl

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.service.Action
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class TestTask extends FunSuite with BeforeAndAfter with MockitoSugar {
  var mockedFeed: Feeder = null
  var mockedAction: Action = null
  var task: Task = null

  before {
    mockedFeed = mock[Feeder]
    mockedAction = mock[Action]

    task = new Task(mockedFeed, mockedAction)
    task.start()
  }

  test("tick") {
    when(mockedFeed.next()).thenReturn(Map("k" -> "val"))

    task.tick(true)
    verify(mockedFeed).next()
    verify(mockedAction).call(anyObject())
  }
}
