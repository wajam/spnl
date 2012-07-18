package com.wajam.spnl

import actors.Actor
import com.wajam.nrv.service.Action
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented

/**
 * Task taking data from a feeder and sending to remote action
 * @param context Task context
 * @param feeder Data source
 * @param action Action to call with new data
 */
class Task(feeder: Feeder, action: Action, var lifetime: TaskLifetime = EPHEMERAL, var name: String = "", var context: TaskContext = new TaskContext) extends Logging with Instrumented {
  val PERSISTENCE_PERIOD = 1000 // if lifetime is persistent, save every 1000ms

  private var beforeThrottleRate = context.rate
  private var throttling = false

  private var persistence: TaskPersistence = null
  private var lastPersistence: Long = 0

  private lazy val tickMeter = metrics.meter("tick", "ticks", name)

  if (lifetime == PERSISTENT_GLOBAL && name.isEmpty)
    throw new UninitializedFieldError("A name should be provided for persistent tasks")

  def rate = context.rate

  def init(persistence: TaskPersistence) {
    this.persistence = persistence
  }

  def start() {
    this.feeder.init(this.context)
    TaskActor.start()
  }

  def kill() {
    this.feeder.kill()
    TaskActor ! Kill
  }

  // actions used by the task actor
  private object Kill

  private object Tick

  object TaskActor extends Actor {
    def throttle() {
      if (!throttling) {
        beforeThrottleRate = context.rate
        context.rate = context.throttleRate
        throttling = true
      }
    }

    def unthrottle() {
      if (throttling) {
        beforeThrottleRate = context.rate
        context.rate = context.throttleRate
        throttling = true
      }
    }

    def act() {
      while (true) {
        receive {
          case Tick =>
            try {
              val data = feeder.next()

              if (data.isDefined) {
                unthrottle()
              } else {
                throttle()
              }

              // trigger persistence if we didn't been saved for PERSISTENCE_PERIOD ms
              val now = System.currentTimeMillis()
              if (lifetime == PERSISTENT_GLOBAL && (now - lastPersistence) >= PERSISTENCE_PERIOD) {
                persistence.saveTask(Task.this)
                lastPersistence = now
              }
            } catch {
              case e: Exception =>
                error("Error calling next on task", e)

                // We got an exception. Handle it like if we didn't have any data by throttling
                throttle()
            }
            sender ! true

          case Kill =>
            throw new InterruptedException()
        }
      }
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

abstract class TaskLifetime

object EPHEMERAL extends TaskLifetime

object PERSISTENT_GLOBAL extends TaskLifetime

