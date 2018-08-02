package com.github.vangj.dp.model

import org.scalatest.{FlatSpec, Matchers}

class MessageTest extends FlatSpec with Matchers {

  "message" should "serialize correctly" in {
    val message = new Message().add("fname", "john").add("lname", "doe")
    val actual = message.toString
    val expected = "{\"lname\":\"doe\",\"fname\":\"john\"}"

    actual should equal(expected)
  }
}
