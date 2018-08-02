package com.github.vangj.bbn.graph.factory

import com.github.vangj.bbn.graph.{Edge, Singly}
import com.github.vangj.bbn.util.MutualInfoUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.util.control.Breaks._

/**
  * Singly-connected graph factory.
  */
object SinglyFactory {

  /**
    * Gets a singly-connected graph from the data frame.
    * @param df Data frame.
    * @return Singly-connected graph.
    */
  def getGraph(df: DataFrame): Singly = {
    val mis = MutualInfoUtil.getMis(df)
        .filter(item => if (item._2._1 == item._2._2) false else true)
        .cache
    getGraph(df, mis)
  }

  def getGraph(df: DataFrame, mis: RDD[(Double, (Int, Int))]): Singly = {
    val numVars = df.columns.length
    getGraph(numVars, mis)
  }

  /**
    * Gets a singly-connected graph based on the mutual
    * information.
    * @param numVars Number of variables.
    * @param mis Mutual information.
    * @return Singly-connected graph.
    */
  def getGraph(numVars: Int, mis: RDD[(Double, (Int, Int))]): Singly = {
    val graph = new Singly
    val total = 0

    breakable {
      mis.collect().foreach(e => {
        graph.addEdge(Edge(e._2._1, e._2._2, false, e._1))

        if (graph.numEdges == total-1) {
          break
        }
      })
    }

    graph
  }
}
