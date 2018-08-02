package com.github.vangj.bbn.graph.json

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.pretty

/**
  * JSON edge.
  * @param parent Parent.
  * @param child Child.
  */
class JsonEdge(parent: Int, child: Int) extends JsonSerializable {
  def json(): JValue = {
    ("parent" -> parent) ~ ("child" -> child)
  }

  override def toString: String = pretty(json())
}
