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


  private implicit val jsonFormats = Serialization.formats(NoTypeHints)

  def toJson: String = {

    val items = List(
      JField("data", JsonCodec.toJValue(data)),
      JField("normalRate", JsonCodec.toJValue(normalRate)),
      JField("throttleRate", JsonCodec.toJValue(throttleRate)),
      JField("maxConcurrent", JsonCodec.toJValue(maxConcurrent)))

    new String(jsonCodec.encode(JObject(items), Encoding))
  }

  def updateFromJson(json: String) {
    val values = jsonCodec.decode(json.getBytes, Encoding).asInstanceOf[JObject].values

    data = values("data").asInstanceOf[Map[String, Any]]
    values.get("normalRate").map(v => normalRate = v.asInstanceOf[Double])
    values.get("throttleRate").map(v => throttleRate = v.asInstanceOf[Double])
    values.get("maxConcurrent").map(v => maxConcurrent = v.asInstanceOf[BigInt].toInt)
  }
}

object TaskContext {
  private val Encoding = "UTF-8"
  private val jsonCodec = new JsonCodec()
}