package com.github.vangj.bbn.graph.json

import org.json4s.JsonAST.JValue

/**
  * Object that can be serialized to JSON.
  */
trait JsonSerializable {
  /**
    * Produces JValue.
    * @return JValue.
    */
  def json(): JValue
}
