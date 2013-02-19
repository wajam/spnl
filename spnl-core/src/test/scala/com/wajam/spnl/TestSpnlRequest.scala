package com.wajam.spnl

import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.mock.MockitoSugar
import com.wajam.nrv.data.InMessage
import org.mockito.Mockito._

/**
 * User: Alexandre Bergeron <alex@wajam.com>
 * Date: 08/11/12
 * Time: 2:13 PM
 */
class TestSpnlRequest extends FunSuite with MockitoSugar with OneInstancePerTest {

  val msg = mock[InMessage]
  val request = new SpnlRequest(msg)
  val e = new Exception()

  test("should send a reply with an ok status") {
    request.ok()
    verify(msg).reply(Map("status" -> "ok"), null, null, 0)
  }

  test("should send a reply with an ignore status") {
    request.ignore(e)
    verify(msg).reply(Map("status" -> "ignore"), null, null, 0)
  }

  test("should send a reply with a failed status") {
    request.fail(e)
    verify(msg).replyWithError(e, Map("status" -> "fail"), null, null, 0)
  }
}
