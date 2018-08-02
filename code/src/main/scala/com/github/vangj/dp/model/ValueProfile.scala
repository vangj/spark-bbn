package com.github.vangj.dp.model

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * Value profile.
  *
  * @param value Value.
  * @param count Total counts.
  */
case class ValueProfile(value: String, count: Long) {
  def json(): JValue = {
    ("value" -> value) ~
    ("count" -> count)
  }

  override def toString: String = pretty(json())
}
