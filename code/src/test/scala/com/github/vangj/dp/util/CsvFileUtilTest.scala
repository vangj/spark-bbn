package com.github.vangj.dp.util

import com.github.vangj.dp.option.CsvParseOptions
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class CsvFileUtilTest extends FlatSpec with Matchers with SharedSparkContext {
  "data with headers" should "parse correctly" in {
    val rawData = List("x1, x2, x3", "t,t,t", "f,f,f")
    val data = sc.parallelize(rawData)
    val options = new CsvParseOptions(true, ',', '"', '\\')
    val headers = CsvFileUtil.getHeaders(options, data)
    headers.size should equal(3)
    headers(0) should equal("x1")
    headers(1) should equal("x2")
    headers(2) should equal("x3")
  }

  "data without headers" should "parse correctly" in {
    val rawData = List("t,t,t", "f,f,f")
    val data = sc.parallelize(rawData)
    val options = new CsvParseOptions(false, ',', '"', '\\')
    val headers = CsvFileUtil.getHeaders(options, data)
    headers.size should equal(3)
    headers(0) should equal("C0")
    headers(1) should equal("C1")
    headers(2) should equal("C2")
  }

  "csv line" should "parse to json" in {
    val line = "a, b, c"
    val json = CsvFileUtil.toJson(line)
    val expected =
      """
        |{"C0" : "a", "C2" : "c", "C1" : "b"}
      """.stripMargin.trim

    //    println(json)
    //    println(expected)

    expected.equals(json) should be(true)
  }

  "csv line" should "parse to json with equal headers" in {
    val line = "a, b, c"
    val headers = Option("x, y, z")
    val json = CsvFileUtil.toJson(line, headers)
    val expected =
      """
        |{"z" : "c", "y" : "b", "x" : "a"}
      """.stripMargin.trim

    //    println(json)
    //    println(expected)

    expected.equals(json) should be(true)
  }

  "csv line" should "parse to json with smaller headers" in {
    val line = "a, b, c"
    val headers = Option("x, y")
    val json = CsvFileUtil.toJson(line, headers)
    val expected =
      """
        |{"y" : "b", "C2" : "c", "x" : "a"}
      """.stripMargin.trim

    //    println(json)
    //    println(expected)

    expected.equals(json) should be(true)
  }

  "csv line" should "parse to json with larger headers" in {
    val line = "a, b, c"
    val headers = Option("x, y, z, w")
    val json = CsvFileUtil.toJson(line, headers)
    val expected =
      """
        |{"z" : "c", "y" : "b", "x" : "a"}
      """.stripMargin.trim

    //    println(json)
    //    println(expected)

    expected.equals(json) should be(true)
  }
}
