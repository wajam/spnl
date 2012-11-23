package com.wajam.spnl

import net.liftweb.json.JsonDSL._
import net.liftweb.json._
import net.liftweb.json.JsonAST.JObject

/**
 * Task running context
 */
case class TaskContext(var data:Map[String, String] = Map[String, String](), var normalRate:Int = 1, var throttleRate:Int = 1) {
  def toJson: String = {
    var json = JObject(List())
    json = json ~ ("normalRate" -> normalRate)
    json = json ~ ("throttleRate" -> throttleRate)
    json = json ~ ("data" -> data)
    Printer.compact(JsonAST.render(json))
  }
}

object TaskContext {
  def fromJson(json: String): TaskContext = {
    implicit val formats = DefaultFormats
    parse(json).extract[TaskContext]
  }
}