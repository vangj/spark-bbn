package com.github.vangj.dp.model

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * Variable profile.
  *
  * @param name Name.
  * @param values List of value profiles.
  */
case class VariableProfile(id: Int, name: String, values: List[ValueProfile]) {
  def json(): JValue = {
    ("id" -> id) ~
    ("name" -> name) ~
    ("values" -> values.map(_.json()))
  }

  override def toString: String = pretty(json())
}
