package com.wajam.spnl

/**
 * Persists tasks and their data
 */
trait TaskPersistence {
  def saveTask(task: Task)

  def loadTask(task: Task)
}
