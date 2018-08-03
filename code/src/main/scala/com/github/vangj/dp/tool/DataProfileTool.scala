package com.github.vangj.dp.tool

import com.github.vangj.dp.factory.DataFrameFactory
import com.github.vangj.dp.util.ProfileUtil
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * Builds data profile.
  * <ul>
  *   <li>i: hdfs path of parquet file used as input</li>
  *   <li>o: hdfs path used as output</li>
  *  </ul>
  */
object DataProfileTool {
  @transient lazy val logger = LogManager.getLogger(DataProfileTool.getClass)

  case class Config(i: String = "", o: String = "")

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("DataProfileTool") {
      head("DataProfileTool", "0.0.1")
      opt[String]("i").required().action( (x, c) => c.copy(i = x)).text("input csv file")
      opt[String]("o").required().action( (x, c) => c.copy(o = x)).text("output hdfs directory")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        try {
          val conf = new SparkConf()
            .setAppName(s"profiles data from (hdfs) ${config.i} to (hdfs) ${config.o}")
          val spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()

          val df = DataFrameFactory.getDataFrame(config.i, spark.sqlContext)
          val variables = ProfileUtil.getProfilesPar(df)
          val profiles = pretty(variables.map(_.json()))

          val profileRdd = spark.sparkContext.parallelize(List(profiles)).persist(StorageLevel.MEMORY_AND_DISK_SER)
//          profileRdd.count
          profileRdd
//            .coalesce(1, true)
            .saveAsTextFile(config.o)

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
