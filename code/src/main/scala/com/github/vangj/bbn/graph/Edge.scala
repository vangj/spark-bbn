package com.github.vangj.bbn.graph

/**
  * An edge.
  * @param i Start node.
  * @param j Stop node.
  * @param directed A boolean indicating if this edge is directed.
  * @param weight A weight associated with the edge.
  */
case class Edge(i: Int, j: Int, directed: Boolean = false, weight: Double = 0)
