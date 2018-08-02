package com.github.vangj.dp.factory

import com.github.vangj.dp.option.CsvParseOptions
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class RDDFactoryTest extends FlatSpec with Matchers with SharedSparkContext {

  "data with headers" should "generate RDD[Map[String, String]] correctly" in {
    val rawData = List("x1, x2, x3", "t,t,t", "f,f,f")
    val data = sc.parallelize(rawData)
    val options = new CsvParseOptions(true, ',', '"', '\\')
    val mapRDD = RDDFactory.getRDD(options, data)

    mapRDD.count() should be(2)
    val list = mapRDD.collect()
    list.size should be(2)

    val map1 = list(0)
    val map2 = list(1)

    map1.size should be(4)
    map2.size should be(4)

    List("id", "x1", "x2", "x3").foreach(field => {
      map1.contains(field) should be(true)
      map2.contains(field) should be(true)
    })

    List("x1", "x2", "x3").foreach(field => {
      map1(field) should be("t")
      map2(field) should be("f")
    })
  }
}
