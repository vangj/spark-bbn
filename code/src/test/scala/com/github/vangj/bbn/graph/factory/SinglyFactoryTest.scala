package com.github.vangj.bbn.graph.factory

import com.github.vangj.bbn.graph.Edge
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class SinglyFactoryTest extends FlatSpec with Matchers with SharedSparkContext {
  "creating a singly-connected graph" should "succeed" in {
    val p1 = (10.0d, (0, 1))
    val p2 = (9.0d, (0, 2))
    val p3 = (8.0d, (1, 2))
    val mis = sc.parallelize(Seq(p1, p2, p3))
    val numVars = 3
    val graph = SinglyFactory.getGraph(numVars, mis)

    graph.numNodes() should be(3)
    graph.getNodes().contains(0) should be(true)
    graph.getNodes().contains(1) should be(true)
    graph.getNodes().contains(2) should be(true)

    graph.numEdges() should be(2)
    graph.getEdges().contains(Edge(0, 1, false, 10.0d))
    graph.getEdges().contains(Edge(0, 2, false, 10.0d))
  }
}
