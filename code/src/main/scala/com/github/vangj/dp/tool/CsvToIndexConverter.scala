package com.github.vangj.dp.tool

import com.github.vangj.dp.factory.RDDFactory
import com.github.vangj.dp.model.Message
import com.github.vangj.dp.option.CsvParseOptions
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Indexes data.
  * <ul>
  *   <li>esnodes: comma-delimited list of elasticsearch nodes</li>
  *   <li>input: hdfs path of CSV file used as input</li>
  *   <li>output: elasticsearch index/type used as output</li>
  *   <li>h: whether the CSV files has headers</li>
  *   <li>e: the escape character</li>
  *   <li>s: the separator character</li>
  *   <li>q: the quote character</li>
  *  </ul>
  */
object CsvToIndexConverter {
  @transient lazy val logger = LogManager.getLogger(CsvToIndexConverter.getClass)

  case class Config(esnodes: String = "", i: String = "", o: String = "",
                    h: Boolean = true, e: Char = '\\', s: Char = ',', q: Char = '\"')

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("CsvToIndexConverter") {
      head("CsvToIndexConverter", "0.0.1")
      opt[String]("esnodes").required().action( (x, c) => c.copy(esnodes = x)).text("elasticsearch nodes")
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
          val conf = new SparkConf()
            .setAppName(s"indexing from (hdfs) ${config.i} to (elasticsearch) ${config.o}")
            .set("es.resource", config.o)
            .set("es.nodes", config.esnodes)
          val spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()

          val csvParseOptions =
            CsvParseOptions(
              config.h,
              config.s,
              config.q,
              config.e)

          val rawData = spark.sparkContext.textFile(config.i)

          RDDFactory.getRDD(csvParseOptions, rawData)
            .saveToEs(config.o, Map("es.nodes" -> config.esnodes, "es.mapping.id" -> "id"))

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
