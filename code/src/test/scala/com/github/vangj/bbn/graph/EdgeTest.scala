package com.github.vangj.bbn.graph

import org.scalatest.{FlatSpec, Matchers}

class EdgeTest extends FlatSpec with Matchers {
  "edge" should "equal each other by indices and type" in {
    val e1 = new Edge(1, 2, false)
    val e2 = new Edge(1, 2, false)
    val e3 = new Edge(1, 2, true)

    e1.equals(e2) should be(true)
    e1.equals(e3) should be(false)
  }

  "set" should "not add duplicate edges" in {
    val e1 = new Edge(1, 2, false)
    val e2 = new Edge(1, 2, false)
    val e3 = new Edge(1, 2, true)

    val set = List(e1, e2, e3).toSet

    set.size should equal(2)
    set.contains(e1) should be(true)
    set.contains(e3) should be(true)
  }
}
