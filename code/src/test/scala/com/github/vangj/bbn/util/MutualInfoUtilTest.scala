package com.github.vangj.bbn.util

import com.github.vangj.bbn.util.MutualInfoUtil.{Combo, Counts, Index}
import com.github.vangj.bbn.util.MutualInfoUtilTest.DataRow
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

object MutualInfoUtilTest {
  case class DataRow(x1: String, x2: String)
}

class MutualInfoUtilTest extends FlatSpec with Matchers with SharedSparkContext {
  "mi computation" should "compute mutual information correctly" in {
    val arr = new ArrayBuffer[String]
    for(_ <- 1 to 57) {
      arr += "a,a"
    }
    for(_ <- 1 to 448) {
      arr += "a,b"
    }
    for(_ <- 1 to 271) {
      arr += "b,a"
    }
    for(_ <- 1 to 224) {
      arr += "b,b"
    }

    val sqlContext = SparkSession.builder.config(sc.getConf).getOrCreate().sqlContext
    import sqlContext.implicits._

    val df = sc.parallelize(arr)
      .map(_.split(","))
      .map(arr => DataRow(arr(0), arr(1)))
      .toDF

    val result = MutualInfoUtil.getMis(df)
      .filter(item => if (item._2._1 == item._2._2) false else true)
      .cache.collect()

    result.length should equal(1)

    val mi = result(0)
    mi._1 should equal(0.11387931780743266 +- 0.00000000001)
    mi._2._1 should equal(0)
    mi._2._2 should equal(1)
  }

  "index" should "show correct equality" in {
    val index1 = Index(0, 1)
    val index2 = Index(0, 1)
    val index3 = Index(0, 2)

    index1.equals(index2) should be(true)
    index2.equals(index1) should be(true)
    index1.equals(index3) should be(false)
    index2.equals(index3) should be(false)
    index3.equals(index1) should be(false)
    index3.equals(index2) should be(false)
  }

  "counts" should "compute mutual information" in {
    val counts = new Counts(Index(0, 1))
    counts.addEntry(Combo("a", "a"), 4)
    counts.addEntry(Combo("a", "b"), 1)
    counts.addEntry(Combo("b", "a"), 2)
    counts.addEntry(Combo("b", "b"), 3)

    counts.total() should be(10.0)

    counts.compute().mi should equal(0.08630462174 +- 0.00000000001)
  }

  "counts" should "add and compute mutual information" in {
    val counts1 = new Counts(Index(0, 1))
    val counts2 = new Counts(Index(0, 1))

    counts1.addEntry(Combo("a", "a"), 4)
    counts1.addEntry(Combo("a", "b"), 1)

    counts2.addEntry(Combo("b", "a"), 2)
    counts2.addEntry(Combo("b", "b"), 3)

    val counts = counts1.add(counts2)
    counts.total should be(10.0)

    counts.compute().mi should be(0.08630462174 +- 0.00000000001)
  }

  "counts" should "compute simple mi" in {
    val counts = new Counts(Index(0, 1))
    counts.addEntry(Combo("a", "a"), 57)
    counts.addEntry(Combo("a", "b"), 448)
    counts.addEntry(Combo("b", "a"), 271)
    counts.addEntry(Combo("b", "b"), 224)

    counts.total() should be(1000.0)

    counts.compute().mi should equal(0.11387931780743266 +- 0.00000000001)
  }
}
