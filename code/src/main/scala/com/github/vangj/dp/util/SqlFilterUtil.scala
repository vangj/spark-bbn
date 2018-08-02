package com.github.vangj.dp.util

import com.github.vangj.dp.model.filter.{CpFilter, CptFilter, CptQueryPlan, UdfFilter}
import com.github.vangj.dp.model.{CondProbFilter, Variable}

/**
  * SQL filter util.
  */
object SqlFilterUtil {

  /**
    * Gets udf filters from the list of variables.
    * @param variables List of variables.
    * @return UDF filters.
    */
  def getCptQueryPlan(variables: List[Variable]): CptQueryPlan = {
    val names = variables.map(v => v.name)
    val values = variables.map(v => v.values)
    getCptQueryPlan(names, values)
  }

  /**
    * Gets a CPT query plan.
    * @param names Names of variables.
    * @param values List of values.
    * @return CPT query plan.
    */
  def getCptQueryPlan(names: List[String], values: List[List[String]]): CptQueryPlan = {
    val qp = new CptQueryPlan()
    qp.add(getCptFilter(names, values))
    qp
  }

  /**
    * Gets CPT filters.
    * @param names List of names.
    * @param values List of values.
    * @return CPT filters.
    */
  def getCptFilter(names: List[String], values: List[List[String]]): CptFilter = {
    val cpFilters = getProduct(values).map(vals => {
      val numerator = new UdfFilter(names, vals)
      val denominator = new UdfFilter(names.tail, vals.tail)
      new CpFilter(numerator, denominator)
    })
    new CptFilter(cpFilters)
  }

  /**
    * Gets the cross-product of the list of values.
    * @param values List of values.
    * @return Cross-product.
    */
  def getProduct(values: List[List[String]]): List[List[String]] = {
    import scalaz.Scalaz._
    def cross(input:List[List[String]]) =
      input.foldLeft(List(List.empty[String]))((l,r) => (l |@| r)(_ :+ _))
    cross(values)
  }

  /**
    * Gets filters used towards computing the conditional probability.
    * @param variables List of variables. This method always assumes that the first
    *                  variable is the variable that is being conditioned for and the
    *                  remaining variables are the variables conditioned on. For example,
    *                  if the list was A, B, then you are attempting to find P(A | B).
    * @return List of filters that can be used towards computing conditional probability.
    */
  def getCondProbFilters(variables: List[Variable]): List[CondProbFilter] = {
    val numerators = getOrderedFilters(variables)
    val denominators = trimFilters(numerators)
    Seq.range(0, numerators.length)
      .map(i => new CondProbFilter(numerators(i), denominators(i)))
      .toList
  }

  /**
    * Removes the first filter element from each filter. For example, if the filter element
    * is <pre>C0='true' and C1='a'</pre> then the first element in this conjunction
    * will be removed and the result will be <pre>C1='a'</pre>.
    * @param filters List of SQL filters.
    * @return List of SQL filters.
    */
  def trimFilters(filters: List[String]): List[String] = {
    filters.map(filter => {
      val indexOfAnd = filter.indexOf("and")
      if (indexOfAnd >= 0) {
        filter.substring(indexOfAnd + 3).trim
      } else {
        filter
      }
    })
  }

  /**
    * Gets a list of SQL filters from the specified list
    * of variables. The SQL filters represent the cross-product
    * of all the values of the variables. The order of the SQL
    * filters are such that they will sum to 1 if normalized
    * according to the first variable's number of values. The filters
    * produced are good for use in parameter learning.
    * @param variables List of variables.
    * @return SQL filters (used in WHERE clause).
    */
  def getOrderedFilters(variables: List[Variable]): List[String] = {
    val filters = getFilters(variables)

    if (filters.length == variables(0).values.length) {
      return filters
    }

    val numValues = variables(0).filters().length
    val product = variables.map(v => v.filters().length).foldLeft(1)((a, b) => a * b)
    val partitionSize = product / variables(0).filters().length

    Seq.range(0, partitionSize)
      .flatMap(i => {
        Seq.range(0, numValues)
          .flatMap(j => {
            val index = i + (j * partitionSize)
            List(filters(index))
          })
      })
      .toList
  }

  /**
    * Gets a list of SQL filters from the specified list of variables. The SQL filters
    * represent the cross-product of all the values of the variables.
    * @param variables List of variables.
    * @return List of SQL filters.
    */
  def getFilters(variables: List[Variable]): List[String] = {
    if (variables.length == 1) {
      variables(0).filters()
    } else {
      variables(0).filters().flatMap(filter => getFilters(1, filter, variables))
    }
  }

  /**
    * Helper method to help recursively build SQL filters that are the result
    * of the cross-product of the values of the variables.
    * @param i Index of current variable.
    * @param filter Current filter state.
    * @param variables List of variables.
    * @return List of SQL filters.
    */
  private def getFilters(i: Int, filter: String, variables: List[Variable]): List[String] = {
    if (i == variables.length-1) {
      variables(i).filters()
        .map(f => {
          filter + " and " + f;
        })
    } else {
      variables(i).filters()
        .flatMap(f => {
          getFilters((i + 1), filter + " and " + f, variables)
        })
    }
  }

}
