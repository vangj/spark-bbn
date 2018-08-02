package com.github.vangj.bbn.graph

import org.scalatest.{FlatSpec, Matchers}

class ChainTest extends FlatSpec with Matchers {
  "creating a graph" should "be successful" in {
    val graph = new Chain
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
    graph.numEdges should be(3)

    graph.getNodes().contains(0) should be(true)
    graph.getNodes().contains(1) should be(true)
    graph.getNodes().contains(2) should be(true)

    graph.getEdges().contains(Edge(0, 1)) should be(true)
    graph.getEdges().contains(Edge(1, 2)) should be (true)
    graph.getEdges().contains(Edge(0, 2)) should be (true)
  }

  "chain graph" should "detect the correct number of nodes and edges" in {
    val graph = new Chain
    graph
      .addEdge(new Edge(1, 2, false))
      .addEdge(new Edge(2, 3, false))
      .addNode(4)

    graph.numNodes should equal(4)
    graph.numEdges should equal(2)
  }

  "chain graph" should "detect edge types" in {
    val graph = new Chain
    graph
      .addEdge(new Edge(1, 2, false))
      .addEdge(new Edge(2, 3, true))
      .addNode(4)

    graph.isDirected(1, 2) should be(false)
    graph.isDirected(2, 3) should be(true)
  }

  "chain graph" should "detect root nodes" in {
    val graph = new Chain
    graph
      .addEdge(new Edge(1, 2, true))
      .addEdge(new Edge(2, 3, true))
      .addNode(4)

    val rootNodes = graph.getRootNodes()
    rootNodes.contains(1) should be(true)
    rootNodes.contains(2) should be(false)
    rootNodes.contains(3) should be(false)
    rootNodes.contains(4) should be(true)
  }

  "chain graph" should "get sets of directed and undirected edges" in {
    val graph = new Chain
    graph
      .addEdge(new Edge(1, 2, true))
      .addEdge(new Edge(2, 3, true))
      .addEdge(new Edge(3, 4, false))

    val setDirected = graph.getDirectedEdges()
    val setUndirected = graph.getUndirectedEdges()

    setDirected.size should equal(2)
    setDirected.contains(new Edge(1, 2, true)) should be(true)
    setDirected.contains(new Edge(2, 3, true)) should be(true)

    setUndirected.size should equal(1)
    setUndirected.contains(new Edge(3, 4, false)) should be(true)
  }

  "chain graph" should "get set of all directed and undirected edges" in {
    val edges = new Chain()
      .addEdge(new Edge(1, 2, true))
      .addEdge(new Edge(2, 3, true))
      .addEdge(new Edge(3, 4, false))
      .getEdges()

    edges.size should equal(3)
    edges.contains(new Edge(1, 2, true)) should be(true)
    edges.contains(new Edge(2, 3, true)) should be(true)
    edges.contains(new Edge(3, 4, false)) should be(true)
  }
}
