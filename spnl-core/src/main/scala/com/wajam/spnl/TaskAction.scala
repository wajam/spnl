package com.wajam.spnl

import com.wajam.nrv.service.{ActionSupportOptions, ActionPath, Action}
import com.wajam.nrv.data.InMessage
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented

/**
 * User: Alexandre Bergeron <alex@wajam.com>
 * Date: 07/11/12
 * Time: 2:13 PM
 */
class TaskAction(val path: ActionPath, val action: Action) extends Logging with Instrumented {

  lazy val name = TaskAction.pathToName(path)

  private lazy val callsMeter = metrics.meter("calls", "calls", name)
  private lazy val successMeter = metrics.meter("success", "success", name)
  private lazy val errorMeter = metrics.meter("error", "error", name)
  private lazy val executeTime = metrics.timer("execute-time", name)

  def this(path: ActionPath, impl: SpnlRequest => Unit, responseTimeout: Long) = {
    this(path, new Action(path, (msg) => impl(new SpnlRequest(msg)),
      actionSupportOptions = new ActionSupportOptions(responseTimeout = Some(responseTimeout))))
  }

  protected[spnl] def processActionResult(task: Task, data: Map[String, Any])
                                         (msg: InMessage, optException: Option[Exception]) {
    optException match {
      case Some(e) => {
        errorMeter.mark()
        log.info("Error occured in task {}: {}", path, e)
        task.fail(data, e)
      }
      case None => {
        successMeter.mark()
        task.tock(data)
      }
    }
  }

  protected[spnl] def call(task: Task, data: Map[String, Any]) {
    callsMeter.mark()
    val timer = executeTime.timerContext()
    action.call(data.toIterable, (message: InMessage, option: Option[Exception]) =>  {
      try {
        processActionResult(task, data)(message, option)
      } finally {
        timer.stop()
      }
    })
  }

}

object TaskAction {
  def pathToName(path: String): String = path.replace("/", "_").substring(1)
}

/**
 * User: Alexandre Bergeron <alex@wajam.com>
 * Date: 08/11/12
 * Time: 11:10 AM
 */
class SpnlRequest(val message: InMessage) extends Logging {

  private val path = message.path

  def ok() {
    log.trace("Success: {}", path)
    message.reply(Map("status" -> "ok"))
  }

  def fail(e: Exception) {
    log.warn("Error occured while processing task {}: {}", path, e)
    message.replyWithError(e, Map("status" -> "fail"))
  }

  def ignore(e: Exception) {
    log.info("Ignored error occured while processing task {}: {}", path, e)
    message.reply(Map("status" -> "ignore"))
  }

}
