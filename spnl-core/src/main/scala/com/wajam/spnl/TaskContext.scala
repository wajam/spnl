package com.wajam.spnl

import net.liftweb.json._
import net.liftweb.json.JsonAST.JObject
import com.wajam.nrv.extension.json.codec.JsonCodec
import com.wajam.spnl.TaskContext._

/**
 * Task running context
 */
case class TaskContext(var data:Map[String, Any] = Map[String, Any](),
                       var normalRate: Double = 1,
                       var throttleRate: Double = 1,
                       var maxConcurrent: Int = 5) {
  def toJson: String = {
    new String(jasonCodec.encode(JObject(List(JField("data", JsonCodec.toJValue(data)))), Encoding))
  }

  def updateFromJson(json: String) {
    val values = jasonCodec.decode(json.getBytes, Encoding).asInstanceOf[JObject].values
    data = values("data").asInstanceOf[Map[String, Any]]
  }
}

object TaskContext {
  private val Encoding = "UTF-8"
  private val jasonCodec = new JsonCodec()
}