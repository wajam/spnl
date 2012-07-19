package com.wajam.spnl

/**
 * Task running context
 */
class TaskContext(var data:Map[String, String] = Map[String, String](), var normalRate:Int = 1, var throttleRate:Int = 1) extends Serializable {
}
