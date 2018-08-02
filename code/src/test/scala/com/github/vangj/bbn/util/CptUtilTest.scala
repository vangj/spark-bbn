package com.github.vangj.bbn.util

import com.github.vangj.bbn.graph.{Bbn, Cpt, Edge}
import com.github.vangj.dp.factory.DataFrameFactory
import com.github.vangj.dp.model.Variable
import com.github.vangj.dp.option.CsvParseOptions
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class CptUtilTest extends FlatSpec with Matchers with SharedSparkContext {
  "cpts" should "be learned correctly for a simple bbn" in {
    val rawData = sc.textFile("src/test/resources/data/cpt-util-test.csv")
    val options = new CsvParseOptions(false, ',', '"', '\\')
    val df = DataFrameFactory.getDataFrame(options, rawData, sc)

    val bbn = new Bbn()
    bbn
      .addNode(0)
      .addNode(1)
      .addEdge(new Edge(0, 1, true))

    val cpts = CptUtil.getCpts(bbn, df)

    val expected = List(
      Cpt(1,Variable("C1",List("a", "b")),List(Variable("C0",List("false", "true"))),List(0.25, 0.75, 0.75, 0.25)),
      Cpt(0,Variable("C0",List("false", "true")),List(),List(0.5, 0.5))
    )

    cpts.size should be(expected.size)
    expected.foreach(e => {
      cpts.contains(e) should be(true)
    })
  }

  "cpts" should "be learned correctly for singly bbn" in {
    val rawData = sc.textFile("src/test/resources/data/data-1479668986461.csv")
    val options = new CsvParseOptions(true, ',', '"', '\\')
    val df = DataFrameFactory.getDataFrame(options, rawData, sc)

    val bbn = new Bbn()
    bbn
      .addNode(0)
      .addNode(1)
      .addNode(2)
      .addNode(3)
      .addNode(4)
      .addEdge(new Edge(0, 1, true))
      .addEdge(new Edge(1, 2, true))
      .addEdge(new Edge(3, 2, true))
      .addEdge(new Edge(3, 4, true))

    val cpts = CptUtil.getCpts(bbn, df)

    val expected = List(
      Cpt(2,Variable("n3",List("false", "true")),List(Variable("n2",List("false", "true")), Variable("n4",List("false", "true"))),List(0.9914285714285713, 0.00857142857142857, 0.3994593202883625, 0.6005406797116375, 0.39842913245269546, 0.6015708675473045, 0.010762975364745275, 0.9892370246352548)),
      Cpt(0,Variable("n1",List("false", "true")),List(),List(0.75065, 0.24935)),
      Cpt(4,Variable("n5",List("maybe", "no", "yes")),List(Variable("n4",List("false", "true"))),List(0.2997143212023351, 0.5976897279841014, 0.10259595081356354, 0.2932462967612352, 0.09649343041258682, 0.6102602728261779)),
      Cpt(1,Variable("n2",List("false", "true")),List(Variable("n1",List("false", "true"))),List(0.7991074402184772, 0.20089255978152265, 0.20473230399037498, 0.7952676960096251)),
      Cpt(3,Variable("n4",List("false", "true")),List(),List(0.40255, 0.59745))
    )

    cpts.size should be(expected.size)
    expected.foreach(e => {
      cpts.contains(e) should be(true)
    })
  }

  "cpts via parallel query" should "be learned correctly for a simple bbn" in {
    val rawData = sc.textFile("src/test/resources/data/cpt-util-test.csv")
    val options = new CsvParseOptions(false, ',', '"', '\\')
    val df = DataFrameFactory.getDataFrame(options, rawData, sc)

    val bbn = new Bbn()
    bbn
      .addNode(0)
      .addNode(1)
      .addEdge(new Edge(0, 1, true))

    val cpts = CptUtil.getCptsPar(bbn, df).groupBy(_.node)

    val expected = List(
      Cpt(1,Variable("C1",List("a", "b")),List(Variable("C0",List("false", "true"))),List(0.25, 0.75, 0.75, 0.25)),
      Cpt(0,Variable("C0",List("false", "true")),List(),List(0.5, 0.5))
    ).groupBy(_.node)

    cpts.size should be(expected.size)
    expected.foreach(e => {
      val id = e._1
      val ex = e._2(0)
      val ob = cpts.get(id).get(0)

      ob.variable should equal(ex.variable)
      ob.parents should equal(ex.parents)

      ob.probs.length should equal(ex.probs.length)
      for (index <- 0 until ob.probs.length) {
        ob.probs(index) should be (ex.probs(index) +- 0.0001)
      }
    })
  }

  "cpts via parallel query" should "be learned correctly for singly bbn" in {
    val rawData = sc.textFile("src/test/resources/data/data-1479668986461.csv")
    val options = new CsvParseOptions(true, ',', '"', '\\')
    val df = DataFrameFactory.getDataFrame(options, rawData, sc)

    val bbn = new Bbn()
    bbn
      .addNode(0)
      .addNode(1)
      .addNode(2)
      .addNode(3)
      .addNode(4)
      .addEdge(new Edge(0, 1, true))
      .addEdge(new Edge(1, 2, true))
      .addEdge(new Edge(3, 2, true))
      .addEdge(new Edge(3, 4, true))

    val cpts = CptUtil.getCptsPar(bbn, df).groupBy(_.node)

    val expected = List(
      Cpt(2,Variable("n3",List("false", "true")),List(Variable("n2",List("false", "true")), Variable("n4",List("false", "true"))),List(0.9914285714285713, 0.00857142857142857, 0.3994593202883625, 0.6005406797116375, 0.39842913245269546, 0.6015708675473045, 0.010762975364745275, 0.9892370246352548)),
      Cpt(0,Variable("n1",List("false", "true")),List(),List(0.75065, 0.24935)),
      Cpt(4,Variable("n5",List("maybe", "no", "yes")),List(Variable("n4",List("false", "true"))),List(0.2997143212023351, 0.5976897279841014, 0.10259595081356354, 0.2932462967612352, 0.09649343041258682, 0.6102602728261779)),
      Cpt(1,Variable("n2",List("false", "true")),List(Variable("n1",List("false", "true"))),List(0.7991074402184772, 0.20089255978152265, 0.20473230399037498, 0.7952676960096251)),
      Cpt(3,Variable("n4",List("false", "true")),List(),List(0.40255, 0.59745))
    ).groupBy(_.node)

    cpts.size should be(expected.size)
    expected.foreach(e => {
      val id = e._1
      val ex = e._2(0)
      val ob = cpts.get(id).get(0)

      ob.variable should equal(ex.variable)
      ob.parents should equal(ex.parents)

      ob.probs.length should equal(ex.probs.length)
      for (index <- 0 until ob.probs.length) {
        ob.probs(index) should be (ex.probs(index) +- 0.0001)
      }
    })
  }
}
