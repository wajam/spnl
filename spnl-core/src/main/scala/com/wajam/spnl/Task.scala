package com.wajam.spnl

import actors.Actor
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import feeder.Feeder

/**
 * Task taking data from a feeder and sending to remote action
 * @param context Task context
 * @param feeder Data source
 * @param action Action to call with new data
 */
class Task(feeder: Feeder, val action: TaskAction, val lifetime: TaskLifetime = EPHEMERAL, val name: String = "", var context: TaskContext = new TaskContext) extends Logging with Instrumented {
  val PERSISTENCE_PERIOD = 1000 // if lifetime is persistent, save every 1000ms

  if (lifetime == PERSISTENT_GLOBAL && name.isEmpty)
    throw new UninitializedFieldError("A name should be provided for persistent tasks")

  private var persistence: TaskPersistence = null
  private var lastPersistence: Long = 0
  private lazy val tickMeter = metrics.meter("tick", "ticks", name)

  @volatile
  var currentRate = context.normalRate

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

  def tock(data: Map[String, Any]) {
    TaskActor ! Tock(data)
  }

  def isThrottling = currentRate < context.normalRate


  // actions used by the task actor
  private object Kill

  private object Tick

  private case class Tock(data: Map[String, Any])

  object TaskActor extends Actor {

    def act() {
      loop {
        react {
          case Tick => {
            try {
              feeder.peek() match {
                case Some(data) => {
                  //TODO check if we can execute job with current token
                  feeder.next()
                  action.call(Task.this, data)
                  currentRate = context.normalRate
                }
                case None => {
                  currentRate = context.throttleRate
                }
              }

              // trigger persistence if we didn't been saved for PERSISTENCE_PERIOD ms
              val now = System.currentTimeMillis()
              if (lifetime == PERSISTENT_GLOBAL && (now - lastPersistence) >= PERSISTENCE_PERIOD) {
                persistence.saveTask(Task.this)
                lastPersistence = now
              }
            } catch {
              case e: Exception =>
                error("Caught an exception while executing the task", e)

                // We got an exception. Handle it like if we didn't have any data by throttling
                currentRate = context.throttleRate
            }
            sender ! true
          }
          case Kill => {
            info("Task {} killed", name)
            exit()
          }
          case Tock(data) => {
            for (token <- data.get("token")) {
              trace("Task finished for token {}", token)
              //TODO free token
            }
            feeder.ack(data)
          }
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

