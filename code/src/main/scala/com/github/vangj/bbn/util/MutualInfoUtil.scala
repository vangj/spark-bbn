package com.github.vangj.bbn.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * Mutual information util.
  */
object MutualInfoUtil {
  case class Index(i: Int, j: Int)

  case class Combo(x: String, y: String)

  case class Result(i: Int, j: Int, mi: Double)

  class Counts(_index: Index) extends Serializable {
    def index: Index = _index

    val counts = new mutable.HashMap[Combo, Int]
    var n = 0.0d

    def compute(): Result = {
      var mi = 0.0d
      counts.foreach(f => {
        val combo = f._1
        if (!combo.x.isEmpty && !combo.y.isEmpty) {
          val N_xy = f._2
          val N_x = counts(Combo(combo.x, ""))
          val N_y = counts(Combo("", combo.y))
          val v = (N_xy / n) * (math.log(N_xy) + math.log(n) - math.log(N_x) - math.log(N_y))
          mi += v
        }
      })
      Result(index.i, index.j, mi)
    }

    def total(): Double = this.n

    def addEntry(combo: Combo, n: Int) {
      increment(combo, n)
      increment(Combo(combo.x, ""), n)
      increment(Combo("", combo.y), n)

      this.n += n
    }

    private def increment(combo: Combo, n: Int) {
      var total = 0
      val v = counts.get(combo)
      if (v.isDefined)
        total = v.get
      total += n
      counts.put(combo, total)
    }

    def add(that: Counts): Counts = {
      if (this.index.i != that.index.i || this.index.j != that.index.j)
        throw new RuntimeException("Indices are different, cannot add counts!")

      val combos = new mutable.HashSet[Combo]
      this.counts.keySet.foreach(combo => combos.add(combo))
      that.counts.keySet.foreach(combo => combos.add(combo))

      val count = new Counts(Index(this.index.i, this.index.j))
      combos.foreach(combo => {
        if (!combo.x.isEmpty && !combo.y.isEmpty) {
          var n1 = 0
          var n2 = 0
          if (this.counts.get(combo).isDefined)
            n1 = this.counts(combo)
          if (that.counts.get(combo).isDefined)
            n2 = that.counts(combo)
          val n = n1 + n2
          count.addEntry(Combo(combo.x, combo.y), n)
        }
      })
      count
    }

    def +(that: Counts): Counts = add(that)
  }

  /**
    * Gets a RDD of mutual information.
    * @param data Data frame.
    * @return RDD of mutual information.
    */
  def getMis(data: DataFrame): RDD[(Double, (Int, Int))] = {
    data
      .rdd
      .flatMap(row => {
        for {
          i <- 0 until row.size
          j <- i until row.size
        } yield {
          val x = row.getString(i).trim
          val y = row.getString(j).trim
          val k = Index(i, j)
          val v = new Counts(k)
          v.addEntry(Combo(x, y), 1)

          (k, v)
        }
      })
      .reduceByKey(_ + _)
      .map(f => {
        val v = f._2
        val r = v.compute()
        (r.mi, (r.i, r.j))
      })
      .sortByKey(ascending = false)
  }

}
