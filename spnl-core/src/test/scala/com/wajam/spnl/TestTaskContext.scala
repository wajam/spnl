package com.wajam.spnl

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._

import net.liftweb.json.JsonDSL._
import net.liftweb.json._
import net.liftweb.json.JsonAST.JObject

class TestTaskContext extends FunSuite {
  test("should save and parse context") {
    val context = TaskContext(Map("1" -> "one", "2" -> "two", "3" -> "three"), 4, 5)
    val json = context.toJson
    val parsed = TaskContext.fromJson(json)
    parsed should be (context)
  }
}
