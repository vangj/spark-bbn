package com.github.vangj.dp.test

import org.apache.spark.{SparkConf, SparkContext}

object TestIterateData extends Serializable {
  case class Index(i: Int, v: String) extends Ordered[Index] {
    def compare(that: Index): Int = {
      val iCompare = this.i.compareTo(that.i)
      if (iCompare != 0) {
        return iCompare
      }
      this.v.compareTo(that.v)
    }
  }

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)

    val conf = new SparkConf
    conf.setAppName(s"Iterate data $input to $output")

    val sc = new SparkContext(conf)
    val rdd = sc.textFile(input)

    val rdd2 = rdd
      .filter(line => line.trim().length > 0)
      .flatMap(line => {
        val arr = line.split(",")
        for {
          i <- 0 until arr.length
        } yield {
          val k = Index(i, arr(i).trim().toLowerCase())
          val v: Long = 1L
          (k, v)
        }
      })
      .reduceByKey(_ + _)
      .sortByKey(ascending = true)
      .saveAsTextFile(output)

    sc.stop()
  }
}
