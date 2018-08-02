package com.github.vangj.bbn.graph

import scala.collection.mutable

/**
  * Bayesian belief network.
  */
class Bbn extends Dag {
  private val cpts = new mutable.HashMap[Int, Cpt]

  def addCpt(cpt: Cpt): Bbn = {
    cpts.put(cpt.node, cpt)
    this
  }

  /**
    * Gets the conditional probablity tables.
    * @return CPTs.
    */
  def getCpts(): List[Cpt] = cpts.values.toList
}
