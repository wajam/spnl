package com.wajam.spnl

import net.liftweb.json._
import net.liftweb.json.JsonAST.JObject
import com.wajam.nrv.extension.json.codec.JsonCodec
import com.wajam.spnl.TaskContext._

/**
 * Task running context
 */
case class TaskContext(var data: ContextData = Map(),
                       var normalRate: Double = 1,
                       var throttleRate: Double = 1,
                       var maxConcurrent: Int = 5,
                       allowSameTokenConcurrency: Boolean = false) {


  private implicit val jsonFormats = Serialization.formats(NoTypeHints)

  def toJson: String = {

    val items = List(
      JField("data", JsonCodec.toJValue(data)),
      JField("normal_rate", JsonCodec.toJValue(normalRate)),
      JField("throttle_rate", JsonCodec.toJValue(throttleRate)),
      JField("max_concurrent", JsonCodec.toJValue(maxConcurrent)))

    new String(jsonCodec.encode(JObject(items), Encoding))
  }

  def updateFromJson(json: String) {
    val values = jsonCodec.decode(json.getBytes, Encoding).asInstanceOf[JObject].values

    data = values("data").asInstanceOf[ContextData]
    values.get("normal_rate").map(v => normalRate = v.asInstanceOf[Double])
    values.get("throttle_rate").map(v => throttleRate = v.asInstanceOf[Double])
    values.get("max_concurrent").map(v => maxConcurrent = v.asInstanceOf[BigInt].toInt)
  }
}

object TaskContext {
  private val Encoding = "UTF-8"
  private val jsonCodec = new JsonCodec()

  type ContextData = Map[String, Any]
}