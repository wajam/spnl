package com.wajam.spnl

import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.service.Action
import com.wajam.nrv.data.InMessage
import org.mockito.Mockito._
import org.mockito.Matchers._


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
  val taskAction = new TaskAction(path, (msg: SpnlRequest) => (), action)
  val msg = mock[InMessage]

  test("should kill task when callback has SpnlKillException") {
    taskAction.callback(task, Map())(msg, Some(SpnlKillException))
    verify(task).kill()
    verify(action, never()).call(any(), any(), any(), any(), anyLong())
  }

  test("should retry task when callback has SpnlThrottleAndRetryException") {
    taskAction.callback(task, Map())(msg, Some(SpnlThrottleAndRetryException))
    verify(task, never()).kill()
    verify(action).call(any(), any(), any(), any(), anyLong())
  }

  test("should do nothing if no exception returned") {
    taskAction.callback(task, Map())(msg, None)
    verify(task, never()).kill()
    verify(action, never()).call(any(), any(), any(), any(), anyLong())
  }
}
