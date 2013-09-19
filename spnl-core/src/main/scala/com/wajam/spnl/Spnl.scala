package com.wajam.spnl

/**
 * Spnl service. Handles feeder registry as well as tasks execution.
 */
class Spnl {
  val scheduler = new Scheduler

  def run(task: Task) {

    // Task is loaded from persistence before starting, so all default (or configuration) will be overridden by
    // the persistence. The default (or configuration)  will effectively be loaded as seed values, and since the task
    // isn't started yet, it will have the proper value from persistence before starting.
    task.persistence.loadTask(task)
    scheduler.startTask(task)
  }

  def stop(task: Task) {
    scheduler.endTask(task)
    task.kill()
  }
}
