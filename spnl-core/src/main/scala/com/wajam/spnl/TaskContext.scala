package com.wajam.spnl

import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST.JObject

/**
 * Task running context
 */
case class TaskContext(var data:Map[String, String] = Map[String, String](),
                       var normalRate:Int = 1,
                       var throttleRate:Int = 1,
                       var maxConcurrent: Int = 5) {
  def toJson: String = {
    var json = JObject(List())
    json = json ~ ("data" -> data)
    Printer.compact(JsonAST.render(json))
  }

  def updateFromJson(json: String) {
    implicit val formats = DefaultFormats
    data = (parse(json) \ "data").extract[Map[String, String]]
  }
}