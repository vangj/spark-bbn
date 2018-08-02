package com.github.vangj.dp.model.filter

import com.github.vangj.dp.model.Variable

class CptFilter(cpFilters: List[CpFilter]) {
  var node: Int = -1
  var variable: Variable = new Variable("", List())
  var parents: List[Variable] = List()
  var probs: List[Double] = List()

  def getCpFilters(): List[CpFilter] = cpFilters

  def setNode(node: Int): Unit = this.node = node
  def setVariable(variable: Variable): Unit = this.variable = variable
  def setParents(parents: List[Variable]): Unit = this.parents = parents
  def setProbs(probs: List[Double]): Unit = this.probs = probs
  def getNode(): Int = node
  def getVariable(): Variable = variable
  def getParents(): List[Variable] = parents
  def getProbs(): List[Double] = probs
}
