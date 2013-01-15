package com.wajam.spnl

import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.service.Action
import com.wajam.nrv.data.InMessage
import org.mockito.Mockito._

/**
 * User: Alexandre Bergeron <alex@wajam.com>
 * Date: 08/11/12
 * Time: 11:52 AM
 */
class TestTaskAction extends FunSuite with MockitoSugar with OneInstancePerTest {

  val task = mock[Task]
  when(task.context).thenReturn(new TaskContext())
  val path = "/test"
  val action = mock[Action]
  val taskAction = new TaskAction(path, action)
  val msg = mock[InMessage]

  test("should call task fail when callback has exception") {
    val e = new Exception
    val data: Map[String, Any] = Map()
    taskAction.processActionResult(task, data)(msg, Some(e))
    verify(task).fail(data, e)
    verifyNoMoreInteractions(task)
    verifyZeroInteractions(action)
  }

  test("should do nothing if no exception returned") {
    val data: Map[String, Any] = Map()
    taskAction.processActionResult(task, data)(msg, None)
    verify(task).tock(data)
    verifyNoMoreInteractions(task)
    verifyZeroInteractions(action)
  }
}
