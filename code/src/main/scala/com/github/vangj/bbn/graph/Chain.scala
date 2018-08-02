package com.github.vangj.bbn.graph

/**
  * Chain graph.
  */
class Chain extends Graph {

  override def getEdges(): Set[Edge] = {
    nodes.flatMap(e => {
        val i = e._1
        e._2
          .map(j => {
            val directed = if (nodes(j).contains(i)) false else true
            if (!directed) {
              val start = i min j
              val stop = i max j
              Edge(start, stop, directed)
            } else {
              Edge(i, j, directed)
            }
          })
          .toList
      })
      .toSet
  }

  /**
    * Gets the directed edges.
    * @return Directed edges.
    */
  def getDirectedEdges(): Set[Edge] = getEdges().filter(_.directed)

  /**
    * Gets the undirected edges.
    * @return Undirected edges.
    */
  def getUndirectedEdges(): Set[Edge] = getEdges().filter(!_.directed)

  override protected def thenAdd(edge: Edge): Unit = {
    if (edge.directed) {
      nodes(edge.i) += edge.j
    } else {
      nodes(edge.i) += edge.j
      nodes(edge.j) += edge.i
    }
  }

  /**
    * Checks to see if two nodes are neighbors.
    * @param i Node.
    * @param j Node.
    * @return A boolean indicating if nodes are neighbors.
    */
  def areNeighbors(i: Int, j: Int): Boolean = nodes(i).contains(j) || nodes(j).contains(i)

  override def getNeighbors(i: Int): Set[Int] = {
    nodes.keySet
      .filter(j => (i != j) && areNeighbors(i, j))
      .toSet
  }

  /**
    * Checks if there is a directed edge from i to j.
    * @param i Node.
    * @param j Node.
    * @return A boolean indicating if there is a directed arc from i to j.
    */
  def isDirected(i: Int, j: Int): Boolean = {
    if (nodes(i).contains(j) && !nodes(j).contains(i)) {
      true
    } else {
      false
    }
  }

  /**
    * Checks if a node has parents.
    * @param i Node.
    * @return A boolean indicating if the node has parents.
    */
  private def hasParents(i: Int): Boolean = {
    nodes
      .filter(e => e._1 != i)
      .foreach(e => {
        if (e._2.contains(i)) {
          return true
        }
      })
    false
  }

  /**
    * Finds the first node that doesn't have a parent to be a root node.
    * @return Root node.
    */
  def getRootNodes(): Set[Int] = {
    nodes.keySet
      .filter(!hasParents(_))
      .toSet
  }

}
