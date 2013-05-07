package com.wajam.spnl

import actors.Actor
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import feeder.Feeder
import com.wajam.nrv.utils.CurrentTime
import Task._

/**
 * Task taking data from a feeder and sending to remote action
 * @param context Task context
 * @param feeder Data source
 * @param action Action to call with new data
 */
class Task(feeder: Feeder, val action: TaskAction, val persistence: TaskPersistence = NoTaskPersistence,
           var context: TaskContext = new TaskContext)
  extends Logging with Instrumented with CurrentTime {

  // Distribute in time persistence between tasks
  private var lastPersistence: Long = System.currentTimeMillis() - util.Random.nextInt(PersistencePeriodInMS)
  private lazy val tickMeter = metrics.meter("tick", "ticks", name)
  private lazy val globalRetryCounter = metrics.counter("retry-count")
  private lazy val retryCounter = metrics.counter("retry-count", name)
  private lazy val concurrentCounter = metrics.counter("concurrent-count", name)
  private lazy val processedCounter = metrics.counter("processed-count", name)

  @volatile
  var currentRate: Double = context.normalRate

  private var currentAttempts: Map[String, Attempt] = Map()

  private def dataToken(data: Map[String, Any]): String = {
    data.getOrElse("token", "").asInstanceOf[String]
  }

  val name = feeder.name

  def start() {
    this.feeder.init(this.context)
    TaskActor.start()
  }

  def kill() {
    this.feeder.kill()
    TaskActor ! Kill
  }

  def tock(data: Map[String, Any]) {
    TaskActor ! Tock(data)
  }

  def fail(data: Map[String, Any], e: Exception) {
    TaskActor ! Error(data, e)
  }

  def isThrottling = currentRate < context.normalRate

  def isRetrying = currentAttempts.values.exists(_.mustRetry)

  // actions used by the task actor
  private object Kill

  private object Tick

  private object Wait

  private case class Tock(data: Map[String, Any])

  private case class Error(data: Map[String, Any], e: Exception)

  /**
   * Attempts are used to call the Action logic.
   * When an Attempt fails to execute its Action (error or timeout),
   * the Attempt will stay stored and will retry to execute its Action
   * after a randomized delay.
   * @param data
   */
  private class Attempt(val data: Map[String, Any]) {
    private var errorCount = 0
    private var retryCount = 0
    private var lastAttemptTime = currentTime
    private var lastErrorTime = 0L

    concurrentCounter += 1

    def onError() {
      errorCount += 1
      lastErrorTime = currentTime
      currentRate = context.throttleRate
    }

    //the time (in ms) before next retry scales exponentially and is affected by a slight random range
    //the formula used will generate the following wait time range (for throttlerate=1) :
    // [1,3], [2,4], [4,6], [8,10], [16,18], [32,34] [64,66]
    def nextRetryTime = lastErrorTime + math.pow(2, retryCount-1).toLong * (1000 / context.throttleRate) +
      (1500 * util.Random.nextFloat()).toLong

    def mustRetry = errorCount > retryCount

    def mustRetryNow = mustRetry && nextRetryTime < currentTime

    def retryNow() {
      globalRetryCounter += 1
      retryCounter += 1
      retryCount += 1
      lastAttemptTime = currentTime

      info("Retry {} of task {} ({})", retryCount, name, dataToken(data))
      action.call(Task.this, data)
    }

    def done() {
      globalRetryCounter -= retryCount
      retryCounter -= retryCount
      retryCount = 0
      errorCount = 0

      concurrentCounter -= 1
      processedCounter += 1

      currentRate = context.normalRate
    }
  }

  private object TaskActor extends Actor {

    def act() {
      loop {
        react {
          case Tick => {
            try {
              // Retry at most one element
              currentAttempts.values.find(_.mustRetryNow) match {
                case Some(attempt) => {
                  try {
                    attempt.retryNow()
                  } catch {
                    case e: Exception =>
                      handleError(attempt.data, e)
                  }
                }
                case None => // No attempt to retry at this time
              }

              // Attempt processing data if have enough concurrent slot
              if (currentAttempts.size < context.maxConcurrent) {
                feeder.peek() match {
                  case Some(data) => {
                    try {
                      val token = dataToken(data)
                      if (!currentAttempts.contains(token)) {
                        currentAttempts += (token -> new Attempt(data))
                        feeder.next()
                        action.call(Task.this, data)
                        currentRate = if (isRetrying) context.throttleRate else context.normalRate
                      }
                    } catch {
                      case e: Exception =>
                        handleError(data, e)
                    }
                  }
                  case None => {
                    currentRate = context.throttleRate
                  }
                }
              }

              // trigger persistence if it hasn't been saved for PersistencePeriodInMS ms
              val now = System.currentTimeMillis()
              if (now - lastPersistence >= PersistencePeriodInMS) {
                persistence.saveTask(Task.this)
                lastPersistence = now
              }
            } catch {
              case e: Exception =>
                error("Task {} error on Tick: {}", name, e)

                // We got an exception. Handle it as if we had no data, by throttling the Task
                currentRate = context.throttleRate
            }
          }
          case Tock(data) => {
            try {
              val token = dataToken(data)
              trace("Task {} ({}) finished", name, token)
              currentAttempts(token).done()
              currentAttempts -= token

              feeder.ack(data)
            } catch {
              case e: Exception =>
                error("Task {} ({}) error on Tock: {}", name, dataToken(data), e)
            }
          }
          case Error(data, e) => {
            try {
              handleError(data, e)
            } catch {
              case e: Exception =>
                error("Task {} ({}) error on Error: {}", name, dataToken(data), e)
            }
          }
          case Wait => {
            reply(true)
          }
          case Kill => {
            info("Task {} stopped", name)
            currentAttempts.values.foreach(_.done())
            exit()
          }
        }
      }
    }

    private def handleError(data: Map[String, Any], e: Exception) {
      val token = dataToken(data)
      val attempt = currentAttempts(token)
      attempt.onError()

      info("Task {} ({}) got an error and will retry in {} ms (data={}): {}", name, token, attempt.nextRetryTime - currentTime, data, e)
    }
  }

  protected[spnl] def tick(sync: Boolean = false) {
    this.tickMeter.mark()

    TaskActor ! Tick
    if (sync)
      TaskActor !? Wait
  }
}

object Task {
  private val PersistencePeriodInMS = 10000
}