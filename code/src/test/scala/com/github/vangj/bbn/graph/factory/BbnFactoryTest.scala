package com.github.vangj.bbn.graph.factory

import com.github.vangj.bbn.graph.{Dag, Edge}
import com.github.vangj.dp.factory.DataFrameFactory
import com.github.vangj.dp.option.CsvParseOptions
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class BbnFactoryTest extends FlatSpec with Matchers with SharedSparkContext {

  "dag" should "convert to bbn" in {
    val dag = new Dag()
    dag
      .addNode(0)
      .addNode(1)
      .addNode(2)
      .addEdge(new Edge(0, 1, true))
      .addEdge(new Edge(1, 2, true))

    val bbn = BbnFactory.getGraph(dag)
    bbn.getNodes().size should be(dag.getNodes().size)
    bbn.getEdges().size should be(dag.getEdges().size)

    dag.getNodes().foreach(node => bbn.getNodes().contains(node) should be(true))
    dag.getEdges().foreach(edge => bbn.getEdges().contains(edge) should be(true))
  }

  "bbn mst learner" should "learn from data" in {
    val rawData = sc.textFile("src/test/resources/data/data-1479668986461.csv")
    val options = new CsvParseOptions(true, ',', '"', '\\')
    val df = DataFrameFactory.getDataFrame(options, rawData, sc)

    val bbn = BbnFactory.getGraph(df)

    bbn.getNodes().size should be(5)
    bbn.getEdges().size should be(4)
    bbn.getCpts().size should be(5)

    val s1 = JsonBbnFactory.getGraph(bbn).toString
    val s2 = Source.fromFile("src/test/resources/data/graph.json").mkString.trim

    //TODO fix the test
//    s1 should equal(s2)
  }
}
