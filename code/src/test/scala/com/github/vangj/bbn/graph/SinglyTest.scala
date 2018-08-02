package com.github.vangj.bbn.graph

import org.scalatest.{FlatSpec, Matchers}

class SinglyTest extends FlatSpec with Matchers {
  "creating a singly-connected graph" should "be successful" in {
    val graph = new Singly
    graph
      .addNode(0)
      .addNode(1)
      .addNode(2)
      .addEdge(0, 1)
      .addEdge(1, 2)
      .addEdge(0, 0)
      .addEdge(2, 0)
      .addEdge(Edge(0, 1))

    graph.numNodes should be(3)
    graph.numEdges should be(2)

    graph.getNodes().contains(0) should be(true)
    graph.getNodes().contains(1) should be(true)
    graph.getNodes().contains(2) should be(true)

    graph.getEdges().contains(Edge(0, 1)) should be(true)
    graph.getEdges().contains(Edge(1, 2)) should be (true)
  }
}
