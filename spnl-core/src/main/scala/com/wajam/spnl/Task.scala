package com.wajam.spnl

import actors.Actor
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import feeder.Feeder
import util.Random

/**
 * Task taking data from a feeder and sending to remote action
 * @param context Task context
 * @param feeder Data source
 * @param action Action to call with new data
 */
class Task(feeder: Feeder, val action: TaskAction, val persistence: TaskPersistence = NoTaskPersistence, val name: String = "",
           var context: TaskContext = new TaskContext, acceptor: TaskAcceptor = new AcceptAllTaskAcceptor)
  extends Logging with Instrumented {

  val PERSISTENCE_PERIOD = 10000

  if (persistence != NoTaskPersistence && name.isEmpty)
    throw new UninitializedFieldError("A name should be provided for persistent tasks")

  // Distribute in time persistence between tasks
  private var lastPersistence: Long = System.currentTimeMillis() - Random.nextInt(PERSISTENCE_PERIOD)
  private lazy val tickMeter = metrics.meter("tick", "ticks", name)

  @volatile
  var currentRate = context.normalRate

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

                  def execute() {
                    feeder.next()
                    action.call(Task.this, data)
                    currentRate = context.normalRate
                  }

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
                }
                case None => {
                  currentRate = context.throttleRate
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
            data.get("token") match {
              case Some(token: String) => {
                trace("Task finished for token {}", token)
                currentTokens -= token
              }
              case None =>
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


