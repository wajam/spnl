package com.wajam.spnl

import actors.Actor
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import feeder.Feeder
import util.Random
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
  private var lastPersistence: Long = System.currentTimeMillis() - Random.nextInt(PERSISTENCE_PERIOD)
  private lazy val tickMeter = metrics.meter("tick", "ticks", name)
  private lazy val globalRetryCounter = metrics.counter("retry")
  private lazy val retryCounter = metrics.counter("retry", name)

  @volatile
  var currentRate: Double = context.normalRate

  private var currentTokens: Set[String] = Set()

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

  // actions used by the task actor
  private object Kill

  private object Tick

  private case class Tock(data: Map[String, Any])

  private case class Error(data: Map[String, Any], e: Exception)

  object TaskActor extends Actor {

    private var retry: Option[Retry] = None

    class Retry(data: Map[String, Any]) {
      // Keep the number of consecutive retries done independently from the metrics retry counters which are
      // shared between multiple tasks
      private var count = 0
      private var lastTry = currentTime
      currentRate = context.throttleRate

      def nextWait = math.pow(2, count).toLong * (1000 / context.throttleRate)

      def tryAgain() {
        if (lastTry + nextWait < currentTime) {
          globalRetryCounter += 1
          retryCounter += 1
          count += 1
          lastTry = currentTime

          info("Retry {} of task {}", count, name)
          action.call(Task.this, data)
        }
      }

      def done() {
        globalRetryCounter -= count
        retryCounter -= count
        count = 0

        currentRate = context.normalRate
      }
    }

    def act() {
      loop {
        react {
          case Tick => {
            try {
              // Retry or get data from feeder
              if (retry.isDefined) {
                retry.get.tryAgain()
              } else {
                feeder.peek() match {
                  case Some(data) => {

                    def execute() {
                      feeder.next()
                      action.call(Task.this, data)
                      currentRate = context.normalRate
                    }

                    try {
                      if (acceptor.accept(data)) {
                        data.get("token") match {
                          case Some(token: String) if currentTokens.contains(token) => {
                            currentRate = context.throttleRate
                          }
                          case Some(token: String) => {
                            currentTokens += token
                            execute()
                          }
                          case None => {
                            execute()
                          }
                        }
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
                error("Caught an exception while executing the task {}: {}", name, e)

                // We got an exception. Handle it like if we didn't have any data by throttling
                currentRate = context.throttleRate
            }
            sender ! true
          }
          case Tock(data) => {
            data.get("token") match {
              case Some(token: String) => {
                trace("Task {} finished for token {}", name, token)
                currentTokens -= token
              }
              case None =>
            }

            // Reset retry
            if (retry.isDefined) {
              info("Resume task {} normally after retries.", name)
              retry.get.done()
              retry = None
            }

            feeder.ack(data)
          }
          case Error(data, e) => {
            handleError(data, e)
          }
          case Kill => {
            info("Task {} stopped", name)
            exit()
          }
        }
      }
    }

    private def handleError(data: Map[String, Any], e: Exception) {
      if (retry.isEmpty) {
        retry = Some(new Retry(data))
      }

      info("Task {} got an error and will retry in {} ms: {}", name, retry.get.nextWait, e)
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


