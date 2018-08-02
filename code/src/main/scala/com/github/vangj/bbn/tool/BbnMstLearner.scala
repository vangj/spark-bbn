package com.github.vangj.bbn.tool

import com.github.vangj.bbn.graph.factory.{BbnFactory, JsonBbnFactory}
import com.github.vangj.bbn.util.MutualInfoUtil
import com.github.vangj.dp.factory.DataFrameFactory
import com.github.vangj.dp.model.Message
import com.github.vangj.dp.util.ProfileUtil
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

/**
  * Learns a singly-connected BBN.
  * <ul>
  *   <li>i: hdfs path of parquet file used as input</li>
  *   <li>o: hdfs path used as output</li>
  *   <li>omi: hdfs path used as output for mutual information</li>
  *  </ul>
  */
object BbnMstLearner {
  @transient lazy val logger = LogManager.getLogger(BbnMstLearner.getClass)

  case class Config(i: String = "", o: String = "", omi: String = "")

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("BbnMstLearner") {
      head("BbnMstLearner", "0.0.1")
      opt[String]("i").required().action( (x, c) => c.copy(i = x)).text("input csv file")
      opt[String]("o").required().action( (x, c) => c.copy(o = x)).text("output hdfs directory")
      opt[String]("omi").required().action( (x, c) => c.copy(omi = x)).text("output hdfs directory for mutual infos")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        try {
          val conf = new SparkConf()
            .setAppName(s"learning singly-connected dag from (parquet) ${config.i} to (hdfs) ${config.o} and ${config.omi}")
          val spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()

          val df = DataFrameFactory.getDataFrame(config.i, spark.sqlContext)
          val allMis = MutualInfoUtil.getMis(df)
          val mis = allMis
            .filter(item => if (item._2._1 == item._2._2) false else true)
            .cache
          val variables = ProfileUtil.getVariablesPar(df)

          val bbn = BbnFactory.getGraph(df, mis, variables)
          val jsonBbn = JsonBbnFactory.getGraph(bbn)

          spark.sparkContext.parallelize(List(jsonBbn.toString))
//            .coalesce(1, false)
            .saveAsTextFile(config.o)

          val serMis = allMis
            .map(item => {
              raw"""{ "mi": ${item._1}, "i": ${item._2._1}, "j": ${item._2._2} }"""
            })
            .persist(StorageLevel.MEMORY_AND_DISK_SER)
//          serMis.count
          serMis
//            .coalesce(1, false)
            .saveAsTextFile(config.omi)

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
