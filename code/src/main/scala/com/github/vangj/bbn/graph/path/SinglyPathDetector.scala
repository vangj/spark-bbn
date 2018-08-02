package com.github.vangj.bbn.graph.path

import com.github.vangj.bbn.graph.Graph

import scala.collection.mutable

/**
  * Path exist algorithm for a singly-connected graph.
  */
class SinglyPathDetector() extends PathDetector {

  override def exists(graph: Graph, start: Int, stop: Int): Boolean = {
    val seen = new mutable.HashSet[Int]
    if (start == stop) true else find(graph, start, stop, seen)
  }

  /**
    * Recursive function to attemp to find a path.
    * @param graph Graph.
    * @param i Current node.
    * @param stop Stop node.
    * @param seen Set of all nodes we've seen so far.
    * @return A boolean indicating if there is a path.
    */
  private def find(graph: Graph, i: Int, stop: Int, seen: mutable.HashSet[Int]): Boolean = {
    val neighbors = graph.getNeighbors(i)
    if (neighbors.contains(stop)) {
      return true
    } else {
      seen += i
      for (j <- neighbors) {
        if (!seen.contains(j) && find(graph, j, stop, seen)) {
          return true
        }
      }
    }
    false
  }
}
