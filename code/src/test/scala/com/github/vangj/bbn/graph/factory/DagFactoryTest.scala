package com.github.vangj.bbn.graph.factory

import com.github.vangj.bbn.graph.{Edge, Singly}
import org.scalatest.{FlatSpec, Matchers}

class DagFactoryTest extends FlatSpec with Matchers {
  "creating a dag from a singly-connected graph" should "succeed" in {
    val singly = new Singly()
    singly
      .addNode(0)
      .addNode(1)
      .addNode(2)
      .addEdge(0, 1)
      .addEdge(1, 2)

    val dag = DagFactory.getGraph(singly)
    dag.getNodes().size should be(3)
    dag.getNodes().contains(0) should be(true)
    dag.getNodes().contains(1) should be(true)
    dag.getNodes().contains(2) should be(true)
    dag.getEdges().size should be(2)
    dag.getEdges().contains(Edge(1, 0, true)) should be(true)
    dag.getEdges().contains(Edge(2, 1, true)) should be(true)
  }
}
