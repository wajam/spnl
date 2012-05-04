package com.wajam.spnl

import collection.mutable
import java.util.concurrent.{TimeUnit, ScheduledThreadPoolExecutor}

/**
 * Task scheduler that sends ticks to all running tasks, based on their running
 * rate.
 */
class Scheduler {
  val POOL_SIZE = 4;

  val scheduledExecutor = new ScheduledThreadPoolExecutor(POOL_SIZE)
  val tasks = mutable.MutableList[ScheduledTask]()

  class ScheduledTask(var realTask:Task, var lastRate:Int = 0, var run:Runnable = null)


  def startTask(task:Task) {
    task.start()

    tasks synchronized {
      tasks += new ScheduledTask(task)
    }
  }

  def endTask(task:Task) {
    // TODO: implement
  }

  // tasks rate checker
  scheduledExecutor.scheduleAtFixedRate(new Runnable {
    def run() {
      var tasksCopy:Seq[ScheduledTask] = null
      tasks.synchronized {
        tasksCopy = for (task <- tasks) yield task
      }

      for (task <- tasksCopy) {
       if (task.realTask.rate != task.lastRate) {
         if (task.run != null)
           scheduledExecutor.remove(task.run)

         task.run = new Runnable {
           def run() {
             task.realTask.tick()
           }
         }

         val time = (1000000000f/int2float(task.realTask.rate)).toLong
         scheduledExecutor.scheduleAtFixedRate(task.run, 0, time, TimeUnit.NANOSECONDS)
         task.lastRate = task.realTask.rate
       }
      }

    }
  }, 0, 100, TimeUnit.MILLISECONDS);
}
