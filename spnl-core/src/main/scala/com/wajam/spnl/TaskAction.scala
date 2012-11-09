package com.wajam.spnl

import com.wajam.nrv.service.{ActionPath, Action}
import com.wajam.nrv.data.InMessage
import com.wajam.nrv.Logging

/**
 * User: Alexandre Bergeron <alex@wajam.com>
 * Date: 07/11/12
 * Time: 2:13 PM
 */
class TaskAction(val path: ActionPath,
                 impl: SpnlRequest => Unit,
                 val action: Action) extends Logging {

  def this(path: ActionPath, impl: SpnlRequest => Unit) = {
    this(path, impl, new Action(path, (msg) => impl(new SpnlRequest(msg))))
  }

  protected[spnl] def processActionResult(task: Task, data: Map[String, Any], retriesLeft: Int)
                              (msg: InMessage, optException: Option[Exception]) {
    optException match {
      case Some(SpnlThrottleAndRetryException) => {
        task.currentRate = task.context.throttleRate
        if (retriesLeft > 0) {
          call(task, data, retriesLeft)
        } else {
          task.kill()
        }
      }
      case Some(SpnlKillException) => {
        task.kill()
      }
      case Some(unknownException) => {
        log.error("Unmanaged exception occured in implementation of task {}", path, unknownException)
        task.kill()
      }
      case None =>
    }
  }

  protected[spnl] def call(task: Task, data: Map[String, Any], retries: Int = 5) {
    action.call(data.toIterable, processActionResult(task, data, retries - 1))
  }

}

/**
 * User: Alexandre Bergeron <alex@wajam.com>
 * Date: 08/11/12
 * Time: 11:10 AM
 */
class SpnlRequest(val message: InMessage) extends Logging {

  private val path = message.path

  def ok() {
    log.trace("Success for path {}", path)
    message.reply(Map("status" -> "ok"))
  }

  def fail(e: Exception) {
    log.warn("Non-critical error occured while processing task {}", path, e)
    message.replyWithError(SpnlKillException, Map("status" -> "fail"))
  }

  def retry(e: Exception) {
    log.warn("Recoverable error occured while processing task {}", path, e)
    message.replyWithError(SpnlThrottleAndRetryException, Map("status" -> "retry"))
  }

  def ignore(e: Exception) {
    log.error("Fatal error occured while processing task {}", path, e)
    message.reply(Map("status" -> "ignore"))
  }

}

private[spnl] object SpnlThrottleAndRetryException extends Exception

private[spnl] object SpnlKillException extends Exception

