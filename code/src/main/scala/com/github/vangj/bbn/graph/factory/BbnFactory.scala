package com.github.vangj.bbn.graph.factory

import com.github.vangj.bbn.graph.{Bbn, Dag}
import com.github.vangj.bbn.util.CptUtil
import com.github.vangj.dp.model.Variable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * BBN factory.
  */
object BbnFactory {

  /**
    * Learns a singly-connected BBN from the data frame.
    * @param df Data frame.
    * @return BBN.
    */
  def getGraph(df: DataFrame): Bbn = {
    val singly = SinglyFactory.getGraph(df)
    val dag = DagFactory.getGraph(singly)
    val bbn = BbnFactory.getGraph(dag)

    CptUtil.getCptsPar(bbn, df)

    bbn
  }

  def getGraph(df: DataFrame, mis: RDD[(Double, (Int, Int))], variables: List[Variable]): Bbn = {
    val singly = SinglyFactory.getGraph(df, mis)
    val dag = DagFactory.getGraph(singly)
    val bbn = BbnFactory.getGraph(dag)

    CptUtil.getCptsPar(bbn, df, variables)

    bbn
  }

  /**
    * Gets a BBN from a DAG.
    * @param dag DAG.
    * @return BBN.
    */
  def getGraph(dag: Dag): Bbn = {
    val bbn = new Bbn()
    dag.getNodes().foreach(bbn.addNode(_))
    dag.getEdges().foreach(bbn.addEdge(_))
    bbn
  }
}
