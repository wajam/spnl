package com.wajam.spnl

import actors.Actor
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import feeder.Feeder
import com.wajam.nrv.utils.CurrentTime

/**
 * Task taking data from a feeder and sending to remote action
 * @param context Task context
 * @param feeder Data source
 * @param action Action to call with new data
 */
class Task(val name: String, feeder: Feeder, val action: TaskAction, val persistence: TaskPersistence = NoTaskPersistence,
           var context: TaskContext = new TaskContext, acceptor: TaskAcceptor = new AcceptAllTaskAcceptor)
  extends Logging with Instrumented with CurrentTime {

  val PERSISTENCE_PERIOD = 10000

  // Distribute in time persistence between tasks
  private var lastPersistence: Long = System.currentTimeMillis() - util.Random.nextInt(PERSISTENCE_PERIOD)
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

  private case class Tock(data: Map[String, Any])

  private case class Error(data: Map[String, Any], e: Exception)

  private class Attempt(data: Map[String, Any]) {
    private var errorCount = 0
    private var retryCount = 0
    private var lastAttemptTime = currentTime

    concurrentCounter += 1

    def onError() {
      errorCount += 1
      currentRate = context.throttleRate
    }

    def nextAttemptTime = lastAttemptTime + math.pow(2, retryCount).toLong * (1000 / context.throttleRate)

    def mustRetry = errorCount > retryCount

    def attemptAgain(): Boolean = {
      if (mustRetry && nextAttemptTime < currentTime) {
        globalRetryCounter += 1
        retryCounter += 1
        retryCount += 1
        lastAttemptTime = currentTime

        info("Retry {} of task {} ({})", retryCount, name, dataToken(data))
        action.call(Task.this, data)
        true
      } else {
        false
      }
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
              currentAttempts.values.exists(_.attemptAgain())

              // Attempt processing data if have enough concurrent slot
              if (currentAttempts.size < context.maxConcurrent) {
                feeder.peek() match {
                  case Some(data) => {
                    try {
                      if (acceptor.accept(data)) {
                        // Data is for this task, process it
                        val token = dataToken(data)
                        if (!currentAttempts.contains(token)) {
                          currentAttempts += (token -> new Attempt(data))
                          feeder.next()
                          action.call(Task.this, data)
                          currentRate = if (isRetrying) context.throttleRate else context.normalRate
                        }
                      } else {
                        // Data is not for us, skip it
                        feeder.next()
                        feeder.ack(data)
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

              // trigger persistence if we didn't been saved for PERSISTENCE_PERIOD ms
              val now = System.currentTimeMillis()
              if (now - lastPersistence >= PERSISTENCE_PERIOD) {
                persistence.saveTask(Task.this)
                lastPersistence = now
              }
            } catch {
              case e: Exception =>
                error("Task {} error on Tick: {}", name, e)

                // We got an exception. Handle it like if we didn't have any data by throttling
                currentRate = context.throttleRate
            } finally {
              sender ! true
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
          case Kill => {
            info("Task {} stopped", name)
            exit()
          }
        }
      }
    }

    private def handleError(data: Map[String, Any], e: Exception) {
      val token = dataToken(data)
      val attempt = currentAttempts(token)
      attempt.onError()

      info("Task {} ({}) got an error and will retry in {} ms: {}", name, token, attempt.nextAttemptTime - currentTime, e)
    }
  }

  protected[spnl] def tick(sync: Boolean = false) {
    this.tickMeter.mark()

    if (!sync)
      TaskActor ! Tick
    else
      TaskActor !? Tick
  }
}


