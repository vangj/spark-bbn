package com.github.vangj.bbn.graph.json

import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * JSON BBN.
  * @param nodes Nodes.
  * @param edges Edges.
  */
class JsonBbn(nodes: List[JsonNode], edges: List[JsonEdge]) extends JsonSerializable {
  def json(): JValue = {
    ("nodes" -> nodes.map(_.json())) ~
    ("edges" -> edges.map(_.json()))
  }

  override def toString: String = pretty(json())
}
