package com.github.vangj.dp.test

import java.util.UUID

import com.opencsv.CSVParser
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object TestIndexCode extends Serializable {
  //~/dev/spark/bin/spark-shell --master spark://dqbox:7077 --packages org.elasticsearch:elasticsearch-spark_2.10:2.3.3,com.opencsv:opencsv:3.8
  def test(): Unit = {
    val conf = new SparkConf()
    conf.set("es.resource", "test/test")
    conf.set("es.nodes", "dqbox")
    val sc = new SparkContext(conf)

    val one = Map("id" -> UUID.randomUUID.toString.replace("-", ""), "fname" -> "john", "lname" -> "doe", "age" -> 25)
    val two = Map("id" -> UUID.randomUUID.toString.replace("-", ""), "fname" -> "jane", "lname" -> "doe", "age" -> 21)
    sc.makeRDD(Seq(one, two)).saveToEs("test/test", Map("es.nodes" -> "dqbox", "es.mapping.id" -> "id"))

    val delimiter = ','
    val quote = '\"'
    val escape = '\\'

    val rawData = sc.textFile("hdfs://dqbox/dataqwiki/account/root/dataset/7ba02fb51d6540f48c4057c16488a99d/landing/data-1479668986461.csv")
    val headerLine = rawData.first()

    val realData = rawData.filter(s => !s.equals(headerLine))
    val headers = (new CSVParser(delimiter, quote, escape))
      .parseLine(headerLine)
      .map(_.trim)
      .toList

    val esData = realData
      .map(line => {
        val values = (new CSVParser(delimiter, quote, escape)).parseLine(line).map(_.trim).toList
        val row = scala.collection.mutable.HashMap[String, String]()

        row += ("id" -> UUID.randomUUID.toString.replace("-", ""))
        for (index <- headers.indices) {
          val k = headers(index)
          val v = values(index)
          row += (k -> v)
        }

        row
      })

    esData.saveToEs("test/test1", Map("es.nodes" -> "dqbox", "es.mapping.id" -> "id"))

    val sqlContext = SparkSession.builder.config(sc.getConf).getOrCreate().sqlContext
    val df = sqlContext.read.format("es").options(Map("es.nodes" -> "dqbox", "pushdown" -> "true", "es.nodes.wan.only" -> "true")).load("test/test1").cache()
    df.count()
    df.where("n1='true'").count()
    df.where("n2='false'").count()
  }

  def testGetProfile(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val index = "root/b407871da87346edac09cfd9927b8ce0"
    val options = Map("es.nodes" -> "dqbox", "pushdown" -> "true", "es.nodes.wan.only" -> "true")
    val df = sqlContext.read.format("es")
      .options(options)
      .load(index)
      .cache()
    val profiles =
      df.columns
          .filter(!_.equals("id"))
          .map(col => {
            val values = df.groupBy(col).count().collect().map(_.getString(0)).toList
            (col, values)
          })
  }
}
