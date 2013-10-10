package com.wajam.spnl

import actors.Actor
import com.wajam.commons.Logging
import com.wajam.nrv.UnavailableException
import com.yammer.metrics.scala.Instrumented
import feeder.Feeder
import Task._
import com.wajam.commons.CurrentTime
import com.yammer.metrics.core.{Counter, Metric, MetricName, MetricsRegistryListener}

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
  private lazy val retryCounter = metrics.counter("retry-count", name)
  private lazy val concurrentCounter = metrics.counter("concurrent-count", name)
  private lazy val processedCounter = metrics.counter("processed-count", name)

  metricsRegistry.addListener(RetryWatcher.RetryMetricsListener)

  private val maxRetryCounter = metrics.gauge("max-retry-count") {
    RetryWatcher.max
  }

  @volatile
  var currentRate: Double = 0.0

  private var currentAttempts: Map[Long, Attempt] = Map()

  val name = feeder.name

  def start() {

    // Make sure currentRate is synced with TaskContext
    currentRate = context.normalRate

    this.feeder.init(this.context)
    TaskActor.start()
  }

  def kill() {
    this.feeder.kill()
    TaskActor ! Kill
  }

  def tock(data: TaskData) {
    TaskActor ! Tock(data)
  }

  def fail(data: TaskData, e: Exception) {
    TaskActor ! Error(data, e)
  }

  def isThrottling = currentRate < context.normalRate

  def isRetrying = currentAttempts.values.exists(_.mustRetry)

  // actions used by the task actor
  private object Kill

  private object Tick

  private object Wait

  private case class Tock(data: TaskData)

  private case class Error(data: TaskData, e: Exception)

  /**
   * Attempts are used to call the Action logic.
   * When an Attempt fails to execute its Action (error or timeout),
   * the Attempt will stay stored and will retry to execute its Action
   * after a randomized delay.
   * @param data
   */
  private class Attempt(val data: TaskData) {
    // At this step data.retryCount must be 0, since this is the first attempt.
    assert(data.retryCount == 0)

    private var retryCount = 0
    private var errorCount = 0
    private var lastAttemptTime = currentTime
    private var lastErrorTime = 0L

    concurrentCounter += 1

    def onError(e: Exception) {
      errorCount += 1
      lastErrorTime = currentTime
      currentRate = context.throttleRate

      e match {
        case _:UnavailableException => updateUsingScatteredRandom
        case _ => updateUsingConsistentRandom

      }
    }

    // The time (in ms) before the next retry attempt scales exponentially and is affected by a slight random range.
    // There are 2 different random delay generator used, depending on the case. The examples bellow assume a throttlerate=1
    @volatile
    var nextRetryTime  = 0L

    // This formula is expected to be used when the servers restart or shut down.
    // The idea is to let the server boot for a few seconds, and then spread all the attempts over a larger random
    // range. If the server is down for a longer period of time, the attempts are exponentially less frequent, and
    // will not generate unnecessary traffic.
    // Here's a sample of the data it will generate at each attempt retry:
    // [6,11], [7,12], [9,14], [13,18], [21,26], [37,42], [69,74], [133, 138], [261, 266]
    def updateUsingScatteredRandom {
      nextRetryTime =
        lastErrorTime +                                                    // Initial timestamp offset
        ExpectedUnavailableTimeInMs +                                      // Baseline Reboot Time
        (math.pow(2, retryCount) * (1000 / context.throttleRate) +         // Exponentially increasing factor
        (ExpectedUnavailableTimeInMs * util.Random.nextFloat())).toLong    // Random factor to scatter attempts
    }

    // This formula is expected to be used in all other cases.
    // The idea is that the server is up, but another reason caused the action to fail. Either the traffic was too much
    // for the server to handle or there was another exception while executing the query. To handle both cases, we
    // quickly attempt multiple retries early on, but add a small random delay after every attempt, spreading the
    // attempts, while still keeping a certain consistency between all retry attempts, giving the server a chance to
    // unban the action by inserting increasingly longer periods of time where no spam is sent for the required
    // token.
    // Here's a sample of the data it will generate at each attempt retry:
    // [1, 2.5], [2, 3.5], [4, 5.5], [8, 9.5], [16, 17.5], [32, 33.5], [64, 65.5], [128, 129.5], [256, 257.5]
    def updateUsingConsistentRandom {
      nextRetryTime =
        lastErrorTime +                                              // initial timestamp offset
        (math.pow(2, retryCount) * (1000 / context.throttleRate) +   // exponentially increasing factor
        (1500 * util.Random.nextFloat())).toLong                     // slight random factor to spread traffic
    }

    def mustRetry = errorCount > retryCount

    def mustRetryNow = mustRetry && nextRetryTime < currentTime

    def retryNow() {
      retryCounter += 1
      retryCount += 1

      lastAttemptTime = currentTime

      info("Retry {} of task {} ({})", retryCount, name, data.token)
      action.call(Task.this, data.copy(retryCount = retryCount))
    }

    def done() {
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
                      val token = data.token
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
              val token = data.token
              trace("Task {} ({}) finished", name, token)
              currentAttempts(token).done()
              currentAttempts -= token

              feeder.ack(data)
            } catch {
              case e: Exception =>
                error("Task {} ({}) error on Tock: {}", name, data.token, e)
            }
          }
          case Error(data, e) => {
            try {
              handleError(data, e)
            } catch {
              case e: Exception =>
                error("Task {} ({}) error on Error: {}", name, data.token, e)
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

    private def handleError(data: TaskData, e: Exception) {
      val token = data.token
      val attempt = currentAttempts(token)
      attempt.onError(e)

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
  // When the Task fails and throws an Unavailable Exception,
  // we assume waiting a short delay is required before
  // the server will be available again
  private val ExpectedUnavailableTimeInMs = 5000
}

case class TaskData(
  token: Long,
  values: Map[String, Any] = Map(),
  retryCount: Int = 0)

object RetryWatcher {

  var retryCounters = Map[MetricName, Counter]()

  def max = retryCounters.valuesIterator.maxBy(_.count).count

  object RetryMetricsListener extends MetricsRegistryListener {

    def onMetricAdded(name: MetricName, metric: Metric) {
      metric match {
        case metric: Counter if name.getName == "retry-count" =>
          synchronized {
            retryCounters += (name -> metric)
          }
        case _ =>
      }
    }

    def onMetricRemoved(name: MetricName) {
      if(name.getName == "retry-count") {
        synchronized {
          retryCounters -= name
        }
      }
    }
  }
}
