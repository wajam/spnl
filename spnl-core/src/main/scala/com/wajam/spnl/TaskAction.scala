package com.wajam.spnl

import com.wajam.nrv.service.{ActionPath, Action}
import com.wajam.nrv.data.InMessage
import com.wajam.nrv.Logging

/**
 * User: Alexandre Bergeron <alex@wajam.com>
 * Date: 07/11/12
 * Time: 2:13 PM
 */
class TaskAction(val path: ActionPath, implementation: ((InMessage) => SpnlResult)) extends Logging {

  val action: Action = new Action(path, innerImpl)

  private def innerImpl(msg: InMessage) {
    implementation(msg) match {
      case SpnlOkResult =>
      case SpnlIgnoreResult(inner) =>
        log.warn("Non-critical error occured while processing task {}", path, inner)
      case SpnlRetryResult(inner) => {
        log.warn("Recoverable error occured while processing task {}", path, inner)
        throw new SpnlThrottleException
      }
      case SpnlFailResult(inner) => {
        log.error("Fatal error occured while processing task {}", path, inner)
        throw new SpnlKillException
      }
    }
  }

  protected[spnl] def callback(task: Task, data: Map[String, Any])(msg: InMessage, optException: Option[Exception]) {
    optException match {
      case Some(e: SpnlThrottleException) => {
        task.currentRate = task.context.throttleRate
        call(task, data)
      }
      case Some(e: SpnlKillException) => {
        task.kill()
      }
      case Some(unknownException) => {
        log.error("Unmanaged exception occured in implementation of task {}", path, unknownException)
        task.kill()
      }
      case None =>
    }
  }

  protected[spnl] def call(task: Task, data: Map[String, Any]) = {
    action.call(data.toIterable, callback(task, data))
  }

  sealed private class SpnlThrottleException extends Exception
  sealed private class SpnlKillException extends Exception
}
