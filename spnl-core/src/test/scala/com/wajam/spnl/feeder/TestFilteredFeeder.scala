package com.wajam.spnl.feeder

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.wajam.spnl.{TaskData, TaskContext}
import Feeder._

@RunWith(classOf[JUnitRunner])
class TestFilteredFeeder extends FunSuite with BeforeAndAfter with MockitoSugar {

  var mockFeeder: Feeder = _

  val feederData = Map("token" -> 0L, "key" -> "value")
  val filteredFeederData = Map("token" -> 0L, "notkey" -> "value")

  before {
    mockFeeder = mock[Feeder]
  }

  test("filtered feeder name should be the same as the wrapped feeder") {
    val name = "name"
    val filteredFilter = mockFeeder.withFilter((data: FeederData) => true)
    when(mockFeeder.name).thenReturn(name)

    assert(filteredFilter.name === name)
  }

  test("filtered feeder init should call wrapped feeder init") {
    val taskContext = new TaskContext()
    val filteredFilter = mockFeeder.withFilter((data: FeederData) => true)

    filteredFilter.init(taskContext)
    verify(mockFeeder).init(taskContext)
  }

  test("filtered feeder kill should call wrapped feeder kill") {
    val filteredFilter =  mockFeeder.withFilter((data: FeederData) => true)

    filteredFilter.kill()
    verify(mockFeeder).kill()
  }

  test("filtered feeder ack should call wrapped feeder ack") {
    val filteredFilter =  mockFeeder.withFilter((data: FeederData) => true)

    filteredFilter.ack(feederData)
    verify(mockFeeder).ack(feederData)
  }

  test("filtered feeder peek method should return None if peek returns None") {
    val filteredFilter = mockFeeder.withFilter((data: FeederData) => data.contains("key"))

    when(mockFeeder.peek()).thenReturn(None)
    assert(filteredFilter.peek() === None)
  }

  test("filtered feeder peek method should return data if peeked data satisfies the predicate") {
    val filteredFilter = mockFeeder.withFilter((data: FeederData) => data.contains("key"))

    when(mockFeeder.peek()).thenReturn(Some(feederData))
    assert(filteredFilter.peek() === Some(feederData))
  }

  test("filtered feeder peek method should return the first data that satisfy the predicate") {
    val filteredFilter = mockFeeder.withFilter((data: FeederData) => data.contains("key"))

    when(mockFeeder.peek()).thenReturn(Some(filteredFeederData), Some(feederData))
    when(mockFeeder.next()).thenReturn(Some(filteredFeederData))
    assert(filteredFilter.peek() === Some(feederData))

    //make sure filtered data was acknowledged to the underlying feeder
    verify(mockFeeder).next()
    verify(mockFeeder).ack(filteredFeederData)
  }

  test("filtered feeder peek method should return None if no data satisfy the predicate") {
    val filteredFilter = mockFeeder.withFilter((data: FeederData) => false)

    when(mockFeeder.peek()).thenReturn(Some(filteredFeederData), Some(feederData), None)
    when(mockFeeder.next()).thenReturn(Some(filteredFeederData), Some(feederData))
    assert(filteredFilter.peek() === None)

    //make sure filtered data was acknowledged to the underlying feeder
    verify(mockFeeder, times(2)).next()
    verify(mockFeeder).ack(filteredFeederData)
    verify(mockFeeder).ack(feederData)
  }

  test("filtered feeder next method should return None if next returns None") {
    val filteredFilter = mockFeeder.withFilter((data: FeederData) => data.contains("key"))

    when(mockFeeder.next()).thenReturn(None)
    assert(filteredFilter.next() === None)
  }

  test("filtered feeder next method should return data if next data satisfies the predicate") {
    val filteredFilter = mockFeeder.withFilter((data: FeederData) => data.contains("key"))

    when(mockFeeder.next()).thenReturn(Some(feederData))
    assert(filteredFilter.next() === Some(feederData))
  }

  test("filtered feeder next method should return the first data that satisfies the predicate") {
    val filteredFilter = mockFeeder.withFilter((data: FeederData) => data.contains("key"))

    when(mockFeeder.next()).thenReturn(Some(filteredFeederData), Some(feederData))
    assert(filteredFilter.next() === Some(feederData))

    //make sure filtered data was acknowledged to the underlying feeder
    verify(mockFeeder).ack(filteredFeederData)
  }

  test("filtered feeder next method should return None if no data satisfies the predicate") {
    val filteredFilter = mockFeeder.withFilter((data: FeederData) => false)

    when(mockFeeder.next()).thenReturn(Some(filteredFeederData), Some(feederData), None)
    assert(filteredFilter.next() === None)

    //make sure filtered data was acknowledged to the underlying feeder
    verify(mockFeeder).ack(filteredFeederData)
    verify(mockFeeder).ack(feederData)
  }

  test("should support OR predicate") {
    val filteredFilter = mockFeeder.withFilter(
      ((data: FeederData) => data.contains("anotherKey")) || ((data: FeederData) => data.contains("key")))

    when(mockFeeder.peek()).thenReturn(Some(filteredFeederData), Some(feederData))
    when(mockFeeder.next()).thenReturn(Some(feederData))

    assert(filteredFilter.peek() === Some(feederData))
  }

  test("should support AND predicate") {
    val filteredFilter = new FilteredFeeder(mockFeeder,
      ((data: FeederData) => data.contains("key")) && ((data: FeederData) => data("key") == "value"))

    when(mockFeeder.peek()).thenReturn(Some(filteredFeederData), Some(feederData))
    when(mockFeeder.next()).thenReturn(Some(feederData))

    assert(filteredFilter.peek() === Some(feederData))
  }
}
