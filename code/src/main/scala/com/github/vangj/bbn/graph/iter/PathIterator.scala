package com.github.vangj.bbn.graph.iter

import com.github.vangj.bbn.graph.Graph

/**
  * Path iterator over graph.
  */
trait PathIterator {

  /**
    * Iterates over the graph.
    * @param graph Graph.
    * @param i Starting node.
    * @param event Event with a path is walked.
    */
  def iterate(graph: Graph, i: Int, event: (Int, Int) => Unit): Unit
}
