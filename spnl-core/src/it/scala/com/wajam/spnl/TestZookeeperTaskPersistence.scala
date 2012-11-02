package com.wajam.spnl

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.nrv.cluster.zookeeper.ZookeeperClient
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.service.Action

@RunWith(classOf[JUnitRunner])
class TestZookeeperTaskPersistence extends FunSuite with MockitoSugar {
  val zkPersistence = new ZookeeperTaskPersistence(new ZookeeperClient("127.0.0.1"))
  val mockFeeder = mock[Feeder]
  val mockAction = mock[Action]

  test("should not save and load ephemeral task") {
    var task = new Task(mockFeeder, mockAction, lifetime = EPHEMERAL)
    task.context.data += ("test" -> "value")
    zkPersistence.saveTask(task)

    task = new Task(mockFeeder, mockAction, lifetime = EPHEMERAL)
    zkPersistence.loadTask(task)

    assert(task.context.data.get("test").isEmpty)
  }

  test("a persisted task should always have a name") {
    intercept[UninitializedFieldError] {
      new Task(mockFeeder, mockAction, lifetime = PERSISTENT_GLOBAL)
    }
  }

  test("should save and load persisted task") {
    var task = new Task(mockFeeder, mockAction, lifetime = PERSISTENT_GLOBAL, name = "ittest_persistence")
    task.context.data += ("test" -> "value")
    zkPersistence.saveTask(task)

    task = new Task(mockFeeder, mockAction, lifetime = PERSISTENT_GLOBAL, name = "ittest_persistence")
    zkPersistence.loadTask(task)

    assert("value".equals(task.context.data.get("test").get))
  }

  test("should not throw an exception when loading unexistant data") {
    val task = new Task(mockFeeder, mockAction, lifetime = PERSISTENT_GLOBAL, name = "ittest_unset")
    zkPersistence.loadTask(task)

    assert(task.context.data.get("test").isEmpty)
  }

}
