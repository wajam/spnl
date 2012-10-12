package com.wajam.spnl

/**
 * Spnl service. Handles feeder registry as well as tasks execution.
 */
class Spnl(val persistence: TaskPersistence) {
  val scheduler = new Scheduler

  def run(task: Task) {
    persistence.loadTask(task)
    task.init(persistence)
    scheduler.startTask(task)
  }

  def stop(task: Task) {
    scheduler.endTask(task)
    task.kill()
  }
}
