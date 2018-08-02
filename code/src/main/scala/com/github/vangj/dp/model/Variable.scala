package com.github.vangj.dp.model

/**
  * Variable.
  * @param name Name.
  * @param values List of values.
  */
case class Variable(name: String, values: List[String]) {

  /**
    * Gets the filters for all the values.
    * @return List of filters.
    */
  def filters(): List[String] = values.map(value => name + "='" + value + "'")
}
