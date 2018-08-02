package com.github.vangj.bbn.graph

import org.scalatest.{FlatSpec, Matchers}

class DagTest extends FlatSpec with Matchers {
  "creating a dag" should "be successful" in {
    val graph = new Dag
    graph
      .addNode(0)
      .addNode(1)
      .addNode(2)
      .addEdge(0, 1)
      .addEdge(1, 2)
      .addEdge(2, 0)

    graph.numNodes should be(3)
    graph.numEdges should be(2)

    graph.getNodes().contains(0) should be(true)
    graph.getNodes().contains(1) should be(true)
    graph.getNodes().contains(2) should be(true)

    graph.getEdges().contains(Edge(0, 1, true)) should be(true)
    graph.getEdges().contains(Edge(1, 2, true)) should be (true)

    graph.getChildren(0).size should be(1)
    graph.getChildren(1).size should be(1)
    graph.getChildren(2).size should be(0)

    graph.getChildren(0).contains(1) should be(true)
    graph.getChildren(1).contains(2) should be(true)

    graph.getParents(0).size should be(0)
    graph.getParents(1).size should be(1)
    graph.getParents(2).size should be(1)

    graph.getParents(1).contains(0) should be(true)
    graph.getParents(2).contains(1) should be(true)
  }

  "dag graph" should "not add cycles between two nodes" in {
    val graph = new Dag
    graph
      .addNode(0)
      .addNode(1)
      .addEdge(0, 1)
      .addEdge(1, 0)

    graph.numNodes should equal(2)
    graph.numEdges should equal(1)
  }

  "dag graph" should "not add cycles between three nodes" in {
    val graph = new Dag
    graph
      .addEdge(0, 1)
      .addEdge(1, 2)
      .addEdge(2, 0)

    graph.numNodes should equal(3)
    graph.numEdges should equal(2)
  }

  "dag graph" should "not add cycles between four nodes" in {
    val graph = new Dag
    graph
      .addEdge(0, 1)
      .addEdge(1, 2)
      .addEdge(2, 3)
      .addEdge(3, 0)
      .addEdge(3, 1)

    graph.numNodes should equal(4)
    graph.numEdges should equal(3)
  }

  "dag graph" should "allow arcs for diverging" in {
    val graph = new Dag
    graph
      .addEdge(0, 1)
      .addEdge(0, 2)
      .addEdge(1, 2)

    graph.numNodes should equal(3)
    graph.numEdges should equal(3)
  }

  "dag graph" should "allow arcs for converging" in {
    val graph = new Dag
    graph
      .addEdge(1, 0)
      .addEdge(2, 0)
      .addEdge(1, 2)

    graph.numNodes should equal(3)
    graph.numEdges should equal(3)
  }
}
