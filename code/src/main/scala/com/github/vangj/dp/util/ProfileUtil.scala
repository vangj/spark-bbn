package com.github.vangj.dp.util

import com.github.vangj.dp.model.{ValueProfile, Variable, VariableProfile}
import org.apache.spark.sql.DataFrame

/**
  * Data profiling utility.
  */
object ProfileUtil {

  /**
    * Gets a list of variables from the specified data frame.
    * @param df Data frame.
    * @return List of variables.
    */
  def getVariables(df: DataFrame): List[Variable] = {
    df.columns
      .map(col => {
        val values = df.groupBy(col).count().collect().map(_.getString(0)).toList.sorted
        Variable(col, values)
      }).toList
  }

  def getVariablesPar(df: DataFrame): List[Variable] = {
    val values =
      df.rdd
      .flatMap(row => {
        Seq.range(0, row.length).map(index => {
          val value = row(index).asInstanceOf[String]
          ((index, value), 1)
        })
      })
      .countByKey()

    Seq.range(0, df.columns.length)
      .map(index => {
        val v = values
          .filter(e => e._1._1 == index)
          .map(e => e._1._2)
          .toList
          .sorted
        Variable(df.columns(index), v)
      }).toList
  }

  /**
    * Gets a list of profiles for all the variables in
    * the specified data frame.
    * @param df Data frame.
    * @return List of variable profiles.
    */
  def getProfiles(df: DataFrame): List[VariableProfile] = {
    Seq.range(0, df.columns.length)
      .map(index => {
        val col = df.columns(index)
        val values = df.groupBy(col)
          .count()
          .collect()
          .map(v => ValueProfile(v.getString(0), v.getLong(1)))
          .toList
        VariableProfile(index, col, values)
      })
      .toList
  }

  def getProfilesPar(df: DataFrame): List[VariableProfile] = {
    val values =
      df.rdd
        .flatMap(row => {
          Seq.range(0, row.length).map(index => {
            val value = row(index).asInstanceOf[String]
            ((index, value), 1)
          })
        })
        .countByKey()

    Seq.range(0, df.columns.length)
      .map(index => {
        val v = values
          .filter(e => e._1._1 == index)
          .map(e => ValueProfile(e._1._2, e._2))
          .toList
        VariableProfile(index, df.columns(index), v)
      }).toList
  }
}
