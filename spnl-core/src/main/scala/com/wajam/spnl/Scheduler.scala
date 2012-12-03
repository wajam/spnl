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
    tasks synchronized {
      for (scheduledTask <- tasks.find(_.realTask == task)) {
        tasks -= scheduledTask
      }
    }
  }

  abstract class TaskRunner extends Runnable {
    var done = false
  }

  // tasks rate checker
  scheduledExecutor.scheduleAtFixedRate(new Runnable {
    def run() {
      var tasksCopy: Seq[ScheduledTask] = null
      tasks.synchronized {
        tasksCopy = for (task <- tasks.toSeq) yield task
      }

      for (task <- tasksCopy) {
        val newRate = task.realTask.currentRate
        if (newRate != task.lastRate) {
          if (task.run != null) {
            task.run.done = true
            scheduledExecutor.remove(task.run)
          }

          task.run = new TaskRunner {
            def run() {
              if (done)
                throw new InterruptedException()

              task.realTask.tick()
            }
          }

          val time = scala.math.max((1000000000f / newRate).toLong, 1)
          scheduledExecutor.scheduleAtFixedRate(task.run, 0, time, TimeUnit.NANOSECONDS)

          task.lastRate = newRate
        }
      }

    }
  }, 0, 100, TimeUnit.MILLISECONDS)
}
