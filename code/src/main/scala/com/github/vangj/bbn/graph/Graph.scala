package com.github.vangj.bbn.graph

import scala.collection.immutable.HashSet
import scala.collection.mutable

/**
  * Graph.
  */
class Graph extends Serializable {
  protected val nodes = new mutable.HashMap[Int, mutable.Set[Int]]

  /**
    * Gets the number of nodes.
    * @return Number of nodes.
    */
  def numNodes(): Int = nodes.size

  /**
    * Gets the number of edges.
    * @return Number of edges.
    */
  def numEdges(): Int = getEdges().size

  /**
    * Gets all the edges.
    * @return All the edges.
    */
  def getEdges(): Set[Edge] = {
    nodes.flatMap(e => {
      val i = e._1
      e._2
        .map(j => {
          val start = i min j
          val stop = i max j
          Edge(start, stop)
        })
        .toList
    })
    .toSet
  }

  /**
    * Gets all the nodes.
    * @return All the nodes.
    */
  def getNodes(): Set[Int] = nodes.map(_._1).toSet

  /**
    * Adds a node.
    * @param i Node.
    * @return Graph.
    */
  def addNode(i: Int): Graph = {
    if (!nodes.contains(i)) {
      nodes.put(i, new mutable.HashSet[Int]())
    }
    this
  }

  /**
    * Adds the specified edge.
    * @param edge Edge.
    * @return Graph
    */
  def addEdge(edge: Edge): Graph = {
    if (edge.i != edge.j) {
      addNode(edge.i)
      addNode(edge.j)

      if (shouldAdd(edge)) thenAdd(edge)
    }
    this
  }

  /**
    * Adds the specified edge (edge will be undirected).
    * @param i Start node.
    * @param j End node.
    * @return Graph.
    */
  def addEdge(i: Int, j: Int): Graph = addEdge(i, j, false)

  /**
    * Adds the specified edge.
    * @param i Start node.
    * @param j End node.
    * @return Graph.
    */
  def addEdge(i: Int, j: Int, directed: Boolean): Graph = {
    addEdge(Edge(i, j, directed))
  }

  /**
    * A method to add an edge.
    * @param edge Edge.
    */
  protected def thenAdd(edge: Edge): Unit = {
    if (!edge.directed) {
      nodes(edge.i) += edge.j
      nodes(edge.j) += edge.i
    } else {
      nodes(edge.i).add(edge.j)
    }

  }

  /**
    * A method to check if the edge should be connected.
    * @param edge Edge.
    * @return A boolean indicating if the edge should be created.
    */
  protected def shouldAdd(edge: Edge): Boolean = true

  /**
    * Gets the neighbors of the specified node.
    * @param i Node.
    * @return Neighbors.
    */
  def getNeighbors(i: Int): Set[Int] =
    if (nodes.contains(i)) nodes(i).toSet else new HashSet[Int]()
}
