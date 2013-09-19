package com.wajam.spnl

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._

import net.liftweb.json.Diff
import net.liftweb.json.JsonAST.JNothing
import net.liftweb.json.JsonParser._

class TestTaskContext extends FunSuite {
  test("parse from json include data") {
    val expected = TaskContext(Map("seq" -> Seq("a", "b", "c"), "num" -> 1234567890, "str" -> "1234567890"))
    val json = """{"data":{"seq":["a","b","c"],"num":1234567890,"str":"1234567890"}}"""
    val actual = TaskContext()
    actual.updateFromJson(json)
    actual should be (expected)
  }

  test("parse from json include extra config") {
    val expected = TaskContext(normalRate=4, throttleRate=5, maxConcurrent=6, data=Map("1" -> "one", "2" -> "two", "3" -> "three"))
    val json = """{"normalRate":4.0,"throttleRate":5.0,"maxConcurrent":6,"data":{"1":"one","2":"two","3":"three"}}"""
    val actual = TaskContext()
    actual.updateFromJson(json)
    actual should be (expected)
  }

  test("parse empty json data") {
    val expected = TaskContext()
    val json = """{"data":{}}"""
    val actual = TaskContext()
    actual.updateFromJson(json)
    actual should be (expected)
  }

  test("save to json") {
    val expected = """{"data":{"seq":["a","b","c"],"num":1234567890,"str":"1234567890"},"normalRate":1.0,"throttleRate":1.0,"maxConcurrent":5}"""
    val task = TaskContext(Map("seq" -> Seq("a", "b", "c"), "num" -> 1234567890, "str" -> "1234567890"))
    val actual = task.toJson

    val diff = Diff.diff(parse(expected), parse(actual))
    diff match {
      case Diff(JNothing, JNothing, JNothing) => // great no diff
      case Diff(changed, added, deleted) => {
        fail("%s is different from %s (changed=%s, added=%s, removed=%s)".format(actual, expected, changed, added, deleted))
      }
    }
  }

  test("generated then parsed objects are the same") {
    val task = TaskContext(normalRate=4, throttleRate=5, maxConcurrent=6, data=Map("1" -> "one", "2" -> "two", "3" -> "three"))

    val generated = task.toJson
    val parsedTask = new TaskContext()
    parsedTask.updateFromJson(generated)

    parsedTask should be(task)
  }
}
