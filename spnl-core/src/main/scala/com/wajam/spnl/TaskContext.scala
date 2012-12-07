package com.wajam.spnl

import net.liftweb.json.JsonDSL._
import net.liftweb.json._
import net.liftweb.json.JsonAST.JObject
import com.sun.xml.internal.ws.api.streaming.XMLStreamWriterFactory.Default

/**
 * Task running context
 */
case class TaskContext(var data:Map[String, String] = Map[String, String](),
                       var normalRate:Int = 1,
                       var throttleRate:Int = 1,
                       var maxConcurrent: Option[Int] = Some(TaskContext.DefaultMaxConcurrent)) {
  def toJson: String = {
    var json = JObject(List())
    json = json ~ ("normalRate" -> normalRate)
    json = json ~ ("throttleRate" -> throttleRate)
    json = json ~ ("maxConcurrent" -> maxConcurrent)
    json = json ~ ("data" -> data)
    Printer.compact(JsonAST.render(json))
  }
}

object TaskContext {
  def fromJson(json: String): TaskContext = {
    implicit val formats = DefaultFormats

    // Ensure max concurrent has a value
    val context = parse(json).extract[TaskContext]
    if (context.maxConcurrent.isEmpty) {
      context.maxConcurrent = Some(DefaultMaxConcurrent)
    }

    context
  }

  val DefaultMaxConcurrent = 5
}