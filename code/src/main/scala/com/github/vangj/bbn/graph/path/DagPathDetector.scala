package com.github.vangj.bbn.graph.path

import com.github.vangj.bbn.graph.{Dag, Graph}

import scala.collection.mutable

/**
  * Path exists algorithm for DAG.
  */
class DagPathDetector extends PathDetector {

  override def exists(graph: Graph, start: Int, stop: Int): Boolean = {
    val seen = new mutable.HashSet[Int]
    if (start == stop) true else find(graph.asInstanceOf[Dag], stop, start, seen)
  }

  /**
    * Recursive function to attemp to find a path.
    * @param graph Graph.
    * @param i Current node.
    * @param stop Stop node.
    * @param seen Set of all nodes we've seen so far.
    * @return A boolean indicating if there is a path.
    */
  private def find(graph: Dag, i: Int, stop: Int, seen: mutable.HashSet[Int]): Boolean = {
    val children = graph.getChildren(i)
    if (children.contains(stop)) {
      return true
    } else {
      seen += i
      for (j <- children) {
        if (!seen.contains(j) && find(graph, j, stop, seen)) {
          return true
        }
      }
    }
    false
  }
}
