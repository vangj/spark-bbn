package com.github.vangj.dp.factory

import java.util.UUID

import com.github.vangj.dp.option.CsvParseOptions
import com.github.vangj.dp.util.CsvFileUtil
import com.opencsv.CSVParser
import org.apache.spark.rdd.RDD

/**
  * RDD factory.
  */
object RDDFactory {

  /**
    * Converts RDD[String] to RDD[Map[String, String]]. The string is assumed to be delimited
    * as a part of CSV file. Each row will be converted to a key-value map, where the keys
    * are headers and values are the field values in the CSV row. If there is no header row,
    * then the headers will be C0, C1, ..., CN.
    * @param csvParseOptions CSV parsing options.
    * @param rawData Raw data.
    * @return RDD where each row is stored as a map (keys are field names, values are field values).
    */
  def getRDD(csvParseOptions: CsvParseOptions, rawData: RDD[String]): RDD[Map[String, String]] = {
    val headers = CsvFileUtil.getHeaders(csvParseOptions, rawData)
    val realData =
      if (csvParseOptions.hasHeaders) {
        val headerLine = rawData.first()
        rawData.filter(!_.equals(headerLine))
      } else {
        rawData
      }
    val mapRDD =
      realData
        .map(line => {
          val values = (new CSVParser(csvParseOptions.delimiter, csvParseOptions.quote, csvParseOptions.escape))
            .parseLine(line)
            .map(_.trim)
            .toList
          val row = scala.collection.mutable.HashMap[String, String]()

          row += ("id" -> id())
          for (index <- headers.indices) {
            val k = headers(index)
            val v = values(index)
            row += (k -> v)
          }

          row.toMap
        })
    mapRDD
  }

  /**
    * Generates a random ID based on UUID.
    * @return Random id.
    */
  def id(): String = {
    UUID.randomUUID.toString.replace("-", "")
  }
}
