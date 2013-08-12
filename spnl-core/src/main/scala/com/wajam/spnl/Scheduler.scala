package com.wajam.spnl

import collection.mutable
import java.util.concurrent.{TimeUnit, ScheduledThreadPoolExecutor}

/**
 * Task scheduler that sends ticks to all running tasks, based on their running
 * rate.
 */
class Scheduler {
  val POOL_SIZE = 4

  val scheduledExecutor = new ScheduledThreadPoolExecutor(POOL_SIZE)
  val tasks = mutable.Set[ScheduledTask]()

  class ScheduledTask(var realTask: Task, var lastRate: Double = 0, var run: TaskRunner = null) {
    override def hashCode() = 2 * realTask.hashCode()

    override def equals(obj: Any) = obj match {
      case st: ScheduledTask => this.realTask == st.realTask
      case _ => false
    }
  }


  def startTask(task: Task) {
    task.start()

    tasks synchronized {
      tasks += new ScheduledTask(task)
    }
  }

  def endTask(task: Task) {
    // Remove sheduled task
    val removedTaskOpt = tasks synchronized {
      val foundTask = tasks.find(_.realTask == task)
      for (scheduledTask <- foundTask) {
        tasks -= scheduledTask
      }
      foundTask
    }

    // Stop removed task
    for (removedTask <- removedTaskOpt) {
      removedTask.synchronized {
        val taskRunner = removedTask.run
        if (taskRunner != null) {
          taskRunner.done = true
          scheduledExecutor.remove(taskRunner)
        }
      }
    }
  }

  abstract class TaskRunner extends Runnable {
    var done = false
  }

  /**
   * This constructor block schedule a new task executed every 100ms.
   *
   * This new task periodically checks and updates the rate at which every ActionTask is executed
   * based on that Task's rate attribute (which toggles between normal rate and throttling rate,
   * depending on the current traffic).
   */
  scheduledExecutor.scheduleAtFixedRate(new Runnable {
    def run() {
      var tasksCopy: Seq[ScheduledTask] = null
      tasks.synchronized {
        tasksCopy = for (task <- tasks.toSeq) yield task
      }

      for (task <- tasksCopy) {

        val realTask = task.realTask

        task.synchronized {

          // Check the new rate. it's either the currently set rate, in order to adjust it. Or it's the normal rate in
          // case the rate was 0 (disabled before and thus no longer updated)
          val newRate =
            if (realTask.currentRate > 0)
              realTask.currentRate
            else
              realTask.context.normalRate

          if (newRate != task.lastRate) {

            val taskRunner = task.run
            if (taskRunner != null) {
              taskRunner.done = true
              scheduledExecutor.remove(taskRunner)
            }

            task.run = new TaskRunner {
              def run() {
                if (done)
                  throw new InterruptedException()

                task.realTask.tick()
              }
            }

            val time = scala.math.max((1000000000f / newRate).toLong, 1)
            //The thread will wait a random delay between [0, time] before starting
            scheduledExecutor.scheduleAtFixedRate(task.run, (time * util.Random.nextFloat()).toLong, time, TimeUnit.NANOSECONDS)

            task.lastRate = newRate
          }
        }
      }

    }
  }, 0, 100, TimeUnit.MILLISECONDS)
}
