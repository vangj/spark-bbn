package com.github.vangj.dp.model.filter

class UdfFilter(variables: List[String], values: List[String]) {
  var count = 0.0d
  def numOfVariables(): Int = variables.length
  def numOfValues(): Int = values.length
  def setCount(count: Double): Unit = this.count = count
  def isEmpty: Boolean = variables.length == 0
  def getArrVariables(): String = "array(" + variables.mkString(",") + ")"
  def getArrValues(): String = "array(" + values.map(s => s"'${s}'").mkString(",") + ")"
  def getUdfFilter(udf: String, alias: String): String = udf + "(" + getArrVariables() + "," + getArrValues() + ") as " + alias
  override def hashCode(): Int = variables.mkString(",").hashCode + values.mkString(",").hashCode
}
