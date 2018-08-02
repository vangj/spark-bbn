package com.github.vangj.bbn.graph

import com.github.vangj.bbn.graph.path.DagPathDetector

/**
  * A directed-acylic graph (DAG).
  */
class Dag extends Graph {

  /**
    * Gets the parents of a node.
    * @param i Node.
    * @return Set of parents.
    */
  def getParents(i: Int): Set[Int] = {
    nodes.keySet
      .filter(j => (j != i) && (nodes(j).contains(i)))
      .toSet
  }

  /**
    * Gets the children of a node.
    * @param i Node.
    * @return Set of children.
    */
  def getChildren(i: Int): Set[Int] = nodes(i).toSet

  override def addEdge(edge: Edge): Graph = {
    if (edge.directed) super.addEdge(edge)
    this
  }

  /**
    * Adds the specified edge (edge will be directed).
    * @param i Start node.
    * @param j End node.
    * @return Graph.
    */
  override def addEdge(i: Int, j: Int): Graph = addEdge(i, j, true)

  override def getEdges(): Set[Edge] = {
    val edges = nodes.flatMap(e => {
      val i = e._1
      val list = e._2
        .map(j => {
          Edge(i, j, directed = true)
        })
        .toList
      list
    })
    .toSet
    edges
  }

  override protected def shouldAdd(edge: Edge): Boolean =
    !new DagPathDetector().exists(this, edge.i, edge.j)
}
