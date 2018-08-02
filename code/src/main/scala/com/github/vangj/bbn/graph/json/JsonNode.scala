package com.github.vangj.bbn.graph.json

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.pretty

/**
  * JSON node.
  * @param id Id.
  * @param name Name.
  * @param values Values.
  * @param cpts CPT values.
  */
class JsonNode(id: Int, name: String, values: List[String], cpts: List[Double]) {
  def json(): JValue = {
    ("id" -> id) ~
    ("name" -> name) ~
    ("values" -> values) ~
    ("cpts" ->  cpts)
  }

  override def toString: String = pretty(json())
}
