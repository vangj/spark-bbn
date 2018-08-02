package com.github.vangj.bbn.graph

import com.github.vangj.bbn.graph.path.SinglyPathDetector

/**
  * Singly-connected graph. A singly-connected graph is one where ignoring the direction
  * of the edges, there is at most one path between any two nodes.
  */
class Singly extends Graph {
  override protected def shouldAdd(edge: Edge): Boolean =
    !new SinglyPathDetector().exists(this, edge.i, edge.j)
}
