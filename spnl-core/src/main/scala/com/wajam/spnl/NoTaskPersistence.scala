package com.wajam.spnl

/**
 * Doesn't persist any task, even if they aren't ephemeral
 */
class NoTaskPersistence extends TaskPersistence {
  def saveTask(task: Task) {}

  def loadTask(task: Task) {}
}
