package com.github.vangj.bbn.graph.path

import com.github.vangj.bbn.graph.Graph

/**
  * Checks if a path exists between the specified nodes.
  */
trait PathDetector {
  /**
    * Checks if a path exists.
    * @param graph Graph.
    * @param start Start node.
    * @param stop Stop node.
    * @return A boolean indicating if a path exists.
    */
  def exists(graph: Graph, start: Int, stop: Int): Boolean
}
