package com.wajam.spnl

import actors.Actor
import com.wajam.nrv.service.{Action, ActionURL}
import com.wajam.nrv.data.OutMessage

/**
 * Task taking data from a feeder and sending to remote action
 */
class Task(feeder:Feeder, action:Action) {
  var rate = Int.MaxValue // iteration per second

  // actions
  private object Kill
  private object Tick

  def start() {
    this.feeder.init()
    TaskActor.start()
  }

  def kill() {
    this.feeder.kill()
    TaskActor ! Kill
  }

  object TaskActor extends Actor {
    def act() {
      while (true) {
        receive {
          case Tick =>
            val data = feeder.next()
            action.call(new OutMessage(data))
            sender ! true

          case Kill =>
            throw new InterruptedException()
        }
      }
    }
  }

  protected[spnl] def tick(sync:Boolean = false) {
    if (!sync)
      TaskActor ! Tick
    else
      TaskActor !? Tick
  }
}

