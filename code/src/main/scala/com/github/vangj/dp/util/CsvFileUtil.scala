package com.github.vangj.dp.util

import com.github.vangj.dp.option.CsvParseOptions
import com.opencsv.CSVParser
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.parsing.json.JSONObject

/**
  * CSV file utils.
  */
object CsvFileUtil {
  def toJson(line: String, headers: Option[String]): String = {
    if(headers.isDefined) {
      toJson(line.split(","), headers.get.split(","))
    } else {
      toJson(line)
    }
  }

  def toJson(line: String): String = {
    toJson(line.split(","))
  }

  def toJson(arr: Array[String]): String = {
    val map = new mutable.HashMap[String, String]()
    var i = 0
    for { v <- arr } {
      val k = "C" + i
      map.put(k, v.trim)
      i = i + 1
    }
    JSONObject(map.toMap).toString()
  }

  def toJson(arr: Array[String], headers: Array[String]): String = {
    val map = new mutable.HashMap[String, String]()

    for(i <- arr.indices) {
      var k = "C" + i
      val v = arr(i).trim

      if(i < headers.length) {
        k = headers(i).trim
      }

      map.put(k, v)
    }

    JSONObject(map.toMap).toString()
  }

  /**
    * Gets the headers from the specified RDD. If the first row does NOT store the headers,
    * then the headers will be C0, C1, ..., CN.
    * @param options CSV parsing options.
    * @param data CSV data.
    * @return List of string, each representing a header name.
    */
  def getHeaders(options: CsvParseOptions, data: RDD[String]): List[String] = {
    val delimiter = options.delimiter
    val quote = options.quote
    val escape = options.escape
    val hasHeaders = options.hasHeaders

    val headers = (new CSVParser(delimiter, quote, escape))
      .parseLine(data.first())
      .map(_.trim)
      .toList

    if (hasHeaders) {
      return headers
    }

    val altHeaders = new mutable.ArrayBuffer[String]
    for (i <- headers.indices) {
      altHeaders += (new StringBuilder()).append('C').append(i).toString()
    }

    altHeaders.toList

  }

}
