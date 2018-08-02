package com.github.vangj.bbn.graph.factory

import com.github.vangj.bbn.graph.Bbn
import com.github.vangj.bbn.graph.json.{JsonBbn, JsonEdge, JsonNode}

/**
  * Json BBN factory.
  */
object JsonBbnFactory {

  /**
    * Converts BBN to serializable JSON version.
    * @param bbn BBN.
    * @return JSON BBN.
    */
  def getGraph(bbn: Bbn): JsonBbn = {
    val edges = bbn.getEdges()
      .map(e => {
        new JsonEdge(e.i, e.j)
      })
      .toList

    val nodes = bbn.getCpts()
      .map(cpt => {
        val id = cpt.node
        val name = cpt.variable.name
        val values = cpt.variable.values
        val cpts = cpt.probs
        new JsonNode(id, name, values, cpts)
      })

    new JsonBbn(nodes, edges)
  }
}
