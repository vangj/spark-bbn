package com.github.vangj.bbn.graph.factory

import com.github.vangj.bbn.graph.iter.SinglyPathIterator
import com.github.vangj.bbn.graph.{Dag, Edge, Singly}

/**
  * DAG factory.
  */
object DagFactory {

  /**
    * Gets a DAG from a singly-connected graph.
    * @param singly Singly-connected graph.
    * @return DAG.
    */
  def getGraph(singly: Singly): Dag = {
    val dag = new Dag()
    val root = singly.getNodes().head
    (new SinglyPathIterator()).iterate(singly, root, (i: Int, j: Int) => {
      dag.addEdge(Edge(i, j, directed = true))
    });
    dag
  }
}
