package com.wajam.spnl

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._

import net.liftweb.json.JsonDSL._
import net.liftweb.json.Diff

//import net.liftweb.json._
import net.liftweb.json.JsonAST.{JNothing, JObject}
import net.liftweb.json.JsonParser._

class TestTaskContext extends FunSuite {
  test("parse from json") {
    val expected = TaskContext(Map("1" -> "one", "2" -> "two", "3" -> "three"), 4, 5, Some(6))
    val json = """{"normalRate":4,"throttleRate":5,"maxConcurrent":6,"data":{"1":"one","2":"two","3":"three"}}"""
    val actual = TaskContext.fromJson(json)
    actual should be (expected)
  }

  test("parse from json missing maxConcurrent") {
    val expected = TaskContext(Map("1" -> "one", "2" -> "two", "3" -> "three"), 4, 5)
    val json = """{"normalRate":4,"throttleRate":5,"data":{"1":"one","2":"two","3":"three"}}"""
    val actual = TaskContext.fromJson(json)
    actual should be (expected)
  }

  test("save to json") {
    val expected = """{"normalRate":4,"throttleRate":5,"maxConcurrent":6,"data":{"1":"one","2":"two","3":"three"}}"""
    val task = TaskContext(Map("1" -> "one", "2" -> "two", "3" -> "three"), 4, 5, Some(6))
    val actual = task.toJson

    val diff = Diff.diff(parse(expected), parse(actual))
    diff match {
      case Diff(JNothing, JNothing, JNothing) => // great no diff
      case Diff(changed, added, deleted) => {
        fail("%s is different from %s (changed=%s, added=%s, removed=%s)".format(actual, expected, changed, added, deleted))
      }
    }
  }
}
