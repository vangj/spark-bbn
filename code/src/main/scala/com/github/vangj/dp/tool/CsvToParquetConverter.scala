package com.github.vangj.dp.tool

import com.github.vangj.dp.factory.DataFrameFactory
import com.github.vangj.dp.model.Message
import com.github.vangj.dp.option.CsvParseOptions
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Converts data to parquet format.
  * <ul>
  *   <li>input: hdfs path of CSV file used as input</li>
  *   <li>output: hdfs path of parquet file</li>
  *   <li>h: whether the CSV files has headers</li>
  *   <li>e: the escape character</li>
  *   <li>s: the separator character</li>
  *   <li>q: the quote character</li>
  *  </ul>
  */
object CsvToParquetConverter {

  @transient lazy val logger = LogManager.getLogger(CsvToParquetConverter.getClass)

  case class Config(i: String = "", o: String = "",
                    h: Boolean = true, e: Char = '\\', s: Char = ',', q: Char = '\"')

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("CsvToParquetConverter") {
      head("CsvToParquetConverter", "0.0.1")
      opt[String]("i").required().action( (x, c) => c.copy(i = x)).text("input csv file")
      opt[String]("o").required().action( (x, c) => c.copy(o = x)).text("output hdfs directory")
      opt[Boolean]("h").required().action( (x, c) => c.copy(h = x)).text("has headers?")
      opt[String]("e").required().action( (x, c) => c.copy(e = x.charAt(0))).text("escape character")
      opt[String]("s").required().action( (x, c) => c.copy(s = x.charAt(0))).text("separator character")
      opt[String]("q").required().action( (x, c) => c.copy(q = x.charAt(0))).text("quote character")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        try {
          val options = new CsvParseOptions(config.h, config.s, config.q, config.e)
          val conf = new SparkConf()
            .setAppName(s"saving (csv) ${config.i} to (parquet) ${config.o}")
          val spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()

          val rawData = spark.sparkContext.textFile(config.i)
          val df = DataFrameFactory.getDataFrame(options, rawData, spark.sparkContext)
          df.write.parquet(config.o)

          spark.stop
        } catch {
          case e: Exception => {
            logger.error(e)
          }
        }
      case None =>
        logger.error("invalid arguments!")
    }
  }
}
