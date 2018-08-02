package com.github.vangj.dp.model

import org.json4s.jackson.Serialization.write

import scala.collection.mutable

/**
  * Message.
  */
class Message {

  private val payload = new mutable.HashMap[String, String]()

  /**
    * Adds an entry.
    * @param k Key.
    * @param v Value.
    * @return Message.
    */
  def add(k: String, v: String): Message = {
    payload.put(k, v)
    this
  }

  override def toString: String = {
    implicit val formats = org.json4s.DefaultFormats
    write(payload)
  }
}
