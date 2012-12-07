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

  private var currentElements: Map[String, Option[Retry]] = Map()

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

  def isRetrying = currentElements.values.exists(_.isDefined)

  // actions used by the task actor
  private object Kill

  private object Tick

  private case class Tock(data: Map[String, Any])

  private case class Error(data: Map[String, Any], e: Exception)

  private class Retry(data: Map[String, Any]) {
    private var count = 0
    private var lastTry = currentTime

    currentRate = context.throttleRate

    def nextWait = math.pow(2, count).toLong * (1000 / context.throttleRate)

    def tryAgain(): Boolean = {
      if (lastTry + nextWait < currentTime) {
        globalRetryCounter += 1
        retryCounter += 1
        count += 1
        lastTry = currentTime

        info("Retry {} of task {} ({})", count, name, dataToken(data))
        action.call(Task.this, data)
        true
      } else {
        false
      }
    }

    def done() {
      globalRetryCounter -= count
      retryCounter -= count
      count = 0

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
              currentElements.values.exists(_ match {
                case Some(retry) => retry.tryAgain()
                case _ => false
              })

              // Try to execute an element if have enough concurrent slot
              if (currentElements.size < context.maxConcurrent) {
                feeder.peek() match {
                  case Some(data) => {
                    try {
                      if (acceptor.accept(data)) {
                        // Data is for this task, process it
                        val token = dataToken(data)
                        if (!currentElements.contains(token)) {
                          concurrentCounter += 1
                          currentElements += (token -> None)
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
              currentElements(token) match {
                case Some(retry) => retry.done()
                case _ =>
              }
              currentElements -= token
              concurrentCounter -= 1
              processedCounter += 1

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
      val retry = currentElements(token) match {
        case Some(r) => r
        case None => {
          val r = new Retry(data)
          currentElements += (token -> Some(r))
          r
        }
      }

      info("Task {} ({}) got an error and will retry in {} ms: {}", name, token, retry.nextWait, e)
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


