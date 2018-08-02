package com.github.vangj.bbn.util

import com.github.vangj.bbn.graph.{Bbn, Cpt}
import com.github.vangj.dp.model.Variable
import com.github.vangj.dp.model.filter.CptQueryPlan
import com.github.vangj.dp.util.{ProfileUtil, SqlFilterUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * CPT util.
  */
object CptUtil {

  /**
    * Gets the CPTs for the BBN. The CPTs are stored in the BBN.
    * @param bbn BBN.
    * @param df Data frame.
    * @return List of CPTs.
    */
  def getCpts(bbn: Bbn, df: DataFrame): List[Cpt] = {
    val variables = ProfileUtil.getVariables(df)
    val N = df.count().asInstanceOf[Double]

    val cpts = bbn.getNodes()
      .map(i => {
        val parents = bbn.getParents(i)
        val paList = parents.toList
        val paVars = paList.map(variables(_))
        val indexes = List(i) ++ paList
        val cptVars = indexes.map(variables(_))
        val filters = SqlFilterUtil.getCondProbFilters(cptVars)
        val probs =
          if (0 == parents.size) {
            filters.map(filter => {
              val p = df.where(filter.numerator).count().asInstanceOf[Double] / N
              p
            })
          } else {
            filters.map(filter => {
              val numer = df.where(filter.numerator).count().asInstanceOf[Double] / N
              val denom = df.where(filter.denominator).count().asInstanceOf[Double] / N
              val p = numer / denom
              p
            })
          }

        new Cpt(i, variables(i), paVars, probs)
      })
      .toList

    cpts.foreach(bbn.addCpt(_))

    cpts
  }

  def getCptsPar(bbn: Bbn, df: DataFrame): List[Cpt] = {
    val variables = ProfileUtil.getVariablesPar(df)
    getCptsPar(bbn, df, variables)
  }

  def getCptsPar(bbn: Bbn, df: DataFrame, variables: List[Variable]): List[Cpt] = {
    val N = df.count().asInstanceOf[Double]

    val eqUdf = "eqUdf"
    val queryPlan = new CptQueryPlan()
    queryPlan.setUdf(eqUdf)

    bbn.getNodes()
      .map(index => {
        val indexes = List(index) ++ bbn.getParents(index).toList
        val vars = indexes.map(variables(_))
        val names = vars.map(_.name)
        val vals  = vars.map(_.values)
        val cptFilter = SqlFilterUtil.getCptFilter(names, vals)
        cptFilter.setNode(index)
        cptFilter.setVariable(variables(index))
        cptFilter.setParents(bbn.getParents(index).map(variables(_)).toList)
        cptFilter
      })
      .foreach(queryPlan.add(_))

    df.sparkSession.sqlContext.udf.register(eqUdf, (v1: Seq[Any], v2: Seq[String]) => if (v1.equals(v2)) 1 else 0)
    df.createOrReplaceTempView("df")

    val sql = "select " + queryPlan.getUdfFilters().mkString(",") + " from df"

    val counts = df.sparkSession.sql(sql).groupBy().sum()
    val row = counts.collect()(0)

    counts.columns.foreach(col => {
      val c = col.substring(4, col.length-1)
      val count = row.getAs[Long](col).toDouble
      queryPlan.updateCount(c, count)
    })

    val cpts = queryPlan.getCptFilters().map(cptFilter => {
      val size = cptFilter.getParents().map(_.values.length).foldRight(1)(_ * _)
      val step = cptFilter.getVariable().values.length
      val p = cptFilter.getCpFilters().map(_.getCondProb(N))
      val s = split(size, size, p)
      val j = join(s)
      val probs = j
      cptFilter.setProbs(probs)
      new Cpt(cptFilter.getNode, cptFilter.getVariable, cptFilter.getParents, cptFilter.getProbs)
    })

    cpts.foreach(bbn.addCpt(_))

    cpts
  }

  private def split(size: Int, step: Int, probs: List[Double]): List[List[Double]] = probs.sliding(size, step).toList
  private def join(probs: List[List[Double]]): List[Double] = {
    val rows = probs.length
    val cols = probs(0).length
    val arr = mutable.ArrayBuffer.empty[Double]

    for {
      c <- 0 until cols
      r <- 0 until rows
    } {
      arr += probs(r)(c)
    }

    arr.toList
  }
}
