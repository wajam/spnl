package com.wajam.spnl

/**
 * Spnl service. Handles feeder registry as well as tasks execution.
 */
class Spnl {
  val scheduler = new Scheduler

  def run(task: Task) {
    task.persistence.loadTask(task)
    scheduler.startTask(task)
  }

  def stop(task: Task) {
    scheduler.endTask(task)
    task.kill()
  }
}
