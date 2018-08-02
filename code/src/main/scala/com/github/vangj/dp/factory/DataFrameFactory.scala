package com.github.vangj.dp.factory

import com.github.vangj.dp.option.CsvParseOptions
import com.github.vangj.dp.util.CsvFileUtil
import com.opencsv.CSVParser
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

/**
  * Data frame factory.
  */
object DataFrameFactory {

  /**
    * Gets a data frame from the specified path pointing to a parquet file.
    * @param path Path to parquet file. Should be hdfs://box/path/to/file
    * @param sqlContext SQLContext.
    * @return Data frame.
    */
  def getDataFrame(path: String, sqlContext: SQLContext): DataFrame = {
    sqlContext.read.parquet(path).cache()
  }

  /**
    * Gets a data frame from elastic search.
    * @param options Elasticsearch options.
    * @param index Elasticsearch index. e.g. "root/cars"
    * @param sqlContext SQL context.
    * @return A data frame.
    */
  def getDataFrame(options: Map[String, String], index: String, sqlContext: SQLContext): DataFrame = {
    val df =
      sqlContext.read.format("es")
        .options(options)
        .load(index)
    df.drop(df.col("id")).cache()
  }

  /**
    * Creates a DataFrame from a RDD[String] representing a CSV dataset.
    * @param sc Spark context.
    * @param options CSV parsing options.
    * @param rawData The raw data.
    * @return A data frame.
    */
  def getDataFrame(options: CsvParseOptions, rawData: RDD[String], sc: SparkContext): DataFrame = {
    val data =
      if (options.hasHeaders) {
        val headers = rawData.first()
        rawData
          .filter(!headers.equals(_))
          .map(
            (new CSVParser(options.delimiter, options.quote, options.escape))
              .parseLine(_)
              .map(_.trim)
              .toList)
          .map(Row.fromSeq(_))
      } else {
        rawData
          .map(
            (new CSVParser(options.delimiter, options.quote, options.escape))
              .parseLine(_)
              .map(_.trim)
              .toList)
          .map(Row.fromSeq(_))
      }

    SparkSession.builder.config(sc.getConf).getOrCreate().sqlContext
      .createDataFrame(data, getSchema(CsvFileUtil.getHeaders(options, rawData))).cache()
  }

  /**
    * Creates a schema from a list of header names. All fields will be string type
    * and nullable.
    * @param headers List of headers.
    * @return Schema.
    */
  private def getSchema(headers: List[String]): StructType = {
    StructType(headers.map(field => StructField(field, StringType, nullable = true)))
  }
}
