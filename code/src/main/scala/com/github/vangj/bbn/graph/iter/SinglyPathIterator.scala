package com.github.vangj.bbn.graph.iter

import com.github.vangj.bbn.graph.{Graph, Singly}

import scala.collection.mutable

/**
  * Path iterator for a singly-connected graph.
  */
class SinglyPathIterator extends PathIterator {
  override def iterate(graph: Graph, i: Int, listener: (Int, Int) => Unit): Unit = {
    val singly = graph.asInstanceOf[Singly]
    val seen = new mutable.HashSet[Int]
    traverse(singly, i, listener, seen)
  }

  /**
    * Traverses a singly-connected graph.
    * @param graph Singly-connected graph.
    * @param i Start node.
    * @param listener Path iterator listener.
    * @param seen Nodes that have been seen before.
    */
  private def traverse(graph: Singly, i: Int, listener: (Int, Int) => Unit, seen: mutable.HashSet[Int]) : Unit = {
    val neighbors = graph.getNeighbors(i)
    seen += i
    for (j <- neighbors) {
      if (!seen.contains(j)) {
        listener(i, j)
        traverse(graph, j, listener, seen)
      }
    }
  }
}
