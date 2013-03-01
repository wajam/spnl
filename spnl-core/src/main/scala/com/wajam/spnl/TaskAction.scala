package com.wajam.spnl

import com.wajam.nrv.service.{Resolver, ActionSupportOptions, ActionPath, Action}
import com.wajam.nrv.data.{MValue, MString, InMessage}
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented

/**
 * User: Alexandre Bergeron <alex@wajam.com>
 * Date: 07/11/12
 * Time: 2:13 PM
 */
class TaskAction(val name: String, val action: Action) extends Logging with Instrumented {

  private lazy val callsMeter = metrics.meter("calls", "calls", name)
  private lazy val successMeter = metrics.meter("success", "success", name)
  private lazy val errorMeter = metrics.meter("error", "error", name)
  private lazy val executeTime = metrics.timer("execute-time", name)

  def this(name: String, impl: SpnlRequest => Unit, responseTimeout: Long) = {
    this(name, new Action(new ActionPath("/%s/:%s".format(name, TaskAction.TokenKey)),
      (msg) => impl(new SpnlRequest(msg)), actionSupportOptions = new ActionSupportOptions(
        responseTimeout = Some(responseTimeout), resolver = Some(TaskAction.TokenResolver))))
  }

  protected[spnl] def processActionResult(task: Task, data: Map[String, Any])
                                         (msg: InMessage, optException: Option[Exception]) {
    optException match {
      case Some(e) => {
        errorMeter.mark()
        info("Error occured in task {}: {}", name, e)
        task.fail(data, e)
      }
      case None => {
        successMeter.mark()
        task.tock(data)
      }
    }
  }

  protected[spnl] def call(task: Task, dataToSend: Map[String, Any]) {
    callsMeter.mark()
    val timer = executeTime.timerContext()

    val params : Map[String, MValue] = Map(TaskAction.TokenKey -> dataToSend(TaskAction.TokenKey).toString)
    action.call(params, data = dataToSend, onReply = (message: InMessage, option: Option[Exception]) =>  {
      try {
        processActionResult(task, dataToSend)(message, option)
      } finally {
        timer.stop()
      }
    })
  }
}

object TaskAction {
  val TokenKey = "token"
  val TokenResolver = new Resolver(tokenExtractor = Resolver.TOKEN_PARAM(TokenKey))
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
    message.reply(Map("status" -> MString("ok")))
  }

  def fail(e: Exception) {
    log.warn("Error occured while processing task {}: {}", path, e)
    message.replyWithError(e, Map("status" -> MString("fail")))
  }

  def ignore(e: Exception) {
    log.info("Ignored error occured while processing task {}: {}", path, e)
    message.reply(Map("status" -> MString("ignore")))
  }

}
