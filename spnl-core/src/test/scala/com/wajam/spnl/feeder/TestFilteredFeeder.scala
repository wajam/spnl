package com.wajam.spnl.feeder

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.wajam.spnl.TaskContext
import com.wajam.spnl.feeder.FilteredFeeder._

@RunWith(classOf[JUnitRunner])
class TestFilteredFeeder extends FunSuite with BeforeAndAfter with MockitoSugar {

  var mockFeeder: Feeder = _

  before {
    mockFeeder = mock[Feeder]
  }

  test("filtered feeder name should be the same as the wrapped feeder") {
    val name = "name"
    val filteredFilter = mockFeeder.withFilter((data: Map[String, Any]) => true)
    when(mockFeeder.name).thenReturn(name)

    assert(filteredFilter.name === name)
  }

  test("filtered feeder init should call wrapped feeder init") {
    val taskContext = new TaskContext()
    val filteredFilter = mockFeeder.withFilter((data: Map[String, Any]) => true)

    filteredFilter.init(taskContext)
    verify(mockFeeder).init(taskContext)
  }

  test("filtered feeder kill should call wrapped feeder kill") {
    val filteredFilter =  mockFeeder.withFilter((data: Map[String, Any]) => true)

    filteredFilter.kill()
    verify(mockFeeder).kill()
  }

  test("filtered feeder ack should call wrapped feeder ack") {
    val data = Map("key" -> "value")
    val filteredFilter =  mockFeeder.withFilter((data: Map[String, Any]) => true)

    filteredFilter.ack(data)
    verify(mockFeeder).ack(data)
  }

  test("filtered feeder peek method should return None if peek returns None") {
    val filteredFilter = mockFeeder.withFilter((data: Map[String, Any]) => data.contains("key"))

    when(mockFeeder.peek()).thenReturn(None)
    assert(filteredFilter.peek() === None)
  }

  test("filtered feeder peek method should return data if peeked data satisfies the predicate") {
    val data = Map("key" -> "value")
    val filteredFilter = mockFeeder.withFilter((data: Map[String, Any]) => data.contains("key"))

    when(mockFeeder.peek()).thenReturn(Some(data))
    assert(filteredFilter.peek() === Some(data))
  }

  test("filtered feeder peek method should return the first data that satisfy the predicate") {
    val filteredData = Map("notkey" -> "value")
    val data = Map("key" -> "value")
    val filteredFilter = mockFeeder.withFilter((data: Map[String, Any]) => data.contains("key"))

    when(mockFeeder.peek()).thenReturn(Some(filteredData), Some(data))
    when(mockFeeder.next()).thenReturn(Some(filteredData))
    assert(filteredFilter.peek() === Some(data))

    //make sure filtered data was acknowledged to the underlying feeder
    verify(mockFeeder).next()
    verify(mockFeeder).ack(filteredData)
  }

  test("filtered feeder peek method should return None if no data satisfy the predicate") {
    val filteredData = Map("notkey" -> "value")
    val data = Map("key" -> "value")
    val filteredFilter = mockFeeder.withFilter((data: Map[String, Any]) => false)

    when(mockFeeder.peek()).thenReturn(Some(filteredData), Some(data), None)
    when(mockFeeder.next()).thenReturn(Some(filteredData), Some(data))
    assert(filteredFilter.peek() === None)

    //make sure filtered data was acknowledged to the underlying feeder
    verify(mockFeeder, times(2)).next()
    verify(mockFeeder).ack(filteredData)
    verify(mockFeeder).ack(data)
  }

  test("filtered feeder next method should return None next returns None") {
    val filteredFilter = mockFeeder.withFilter((data: Map[String, Any]) => data.contains("key"))

    when(mockFeeder.next()).thenReturn(None)
    assert(filteredFilter.next() === None)
  }

  test("filtered feeder next method should return data if next data satisfies the predicate") {
    val data = Map("key" -> "value")
    val filteredFilter = mockFeeder.withFilter((data: Map[String, Any]) => data.contains("key"))

    when(mockFeeder.next()).thenReturn(Some(data))
    assert(filteredFilter.next() === Some(data))
  }

  test("filtered feeder next method should return the first data that satisfy the predicate") {
    val filteredData = Map("notkey" -> "value")
    val data = Map("key" -> "value")
    val filteredFilter = mockFeeder.withFilter((data: Map[String, Any]) => data.contains("key"))

    when(mockFeeder.next()).thenReturn(Some(filteredData), Some(data))
    assert(filteredFilter.next() === Some(data))

    //make sure filtered data was acknowledged to the underlying feeder
    verify(mockFeeder).ack(filteredData)
  }

  test("filtered feeder next method should return None if no data satisfy the predicate") {
    val filteredData = Map("notkey" -> "value")
    val data = Map("key" -> "value")
    val filteredFilter = mockFeeder.withFilter((data: Map[String, Any]) => false)

    when(mockFeeder.next()).thenReturn(Some(filteredData), Some(data), None)
    assert(filteredFilter.next() === None)

    //make sure filtered data was acknowledged to the underlying feeder
    verify(mockFeeder).ack(filteredData)
    verify(mockFeeder).ack(data)
  }

  test("should support OR predicate") {
    val filteredData = Map("filtered" -> "value")
    val data = Map("key" -> "value")
    val filteredFilter = mockFeeder.withFilter(
      ((data: Map[String, Any]) => data.contains("key")).
      or((data: Map[String, Any]) => data.contains("notKey")))

    when(mockFeeder.peek()).thenReturn(Some(filteredData), Some(data))
    when(mockFeeder.next()).thenReturn(Some(data))

    assert(filteredFilter.peek() === Some(data))
  }

  test("should support AND predicate") {
    val filteredData = Map("filtered" -> "value")
    val data = Map("key" -> "value")
    val filteredFilter = new FilteredFeeder(mockFeeder,
      ((data: Map[String, Any]) => data.contains("key")).and((data: Map[String, Any]) => data("key") == "value"))

    when(mockFeeder.peek()).thenReturn(Some(filteredData), Some(data))
    when(mockFeeder.next()).thenReturn(Some(data))

    assert(filteredFilter.peek() === Some(data))
  }
}
