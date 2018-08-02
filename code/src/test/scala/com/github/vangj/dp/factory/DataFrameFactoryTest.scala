package com.github.vangj.dp.factory

import com.github.vangj.dp.option.CsvParseOptions
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class DataFrameFactoryTest extends FlatSpec with Matchers with SharedSparkContext {
  "data with headers" should "generate data frame correctly" in {
    val rawData = List("x1, x2, x3", "t,t,t", "f,f,f")
    val data = sc.parallelize(rawData)
    val options = new CsvParseOptions(true, ',', '"', '\\')
    val df = DataFrameFactory.getDataFrame(options, data, sc)
    df.count() should be(2)
    df.where("x1='t'").count() should be(1L)
    df.where("x1='f'").count() should be(1L)
    df.where("x2='t'").count() should be(1L)
    df.where("x2='f'").count() should be(1L)
    df.where("x3='t'").count() should be(1L)
    df.where("x3='f'").count() should be(1L)
  }

  "data without headers" should "generate data frame correctly" in {
    val rawData = List("t,t,t", "f,f,f")
    val data = sc.parallelize(rawData)
    val options = new CsvParseOptions(false, ',', '"', '\\')
    val df = DataFrameFactory.getDataFrame(options, data, sc)
    df.count() should be(2)
    df.where("C0='t'").count() should be(1L)
    df.where("C0='f'").count() should be(1L)
    df.where("C1='t'").count() should be(1L)
    df.where("C1='f'").count() should be(1L)
    df.where("C2='t'").count() should be(1L)
    df.where("C2='f'").count() should be(1L)
  }
}
