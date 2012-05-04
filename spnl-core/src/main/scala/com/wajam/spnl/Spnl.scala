package com.wajam.spnl

import scala.collection.mutable

/**
 * Spnl service. Handles feeder registry as well as tasks execution.
 */
class Spnl {
  var runningTasks = new mutable.LinkedList[Task]()

  def run(task: Task) {
    runningTasks :+= task
    task.start()
  }
}
