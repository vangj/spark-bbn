package com.github.vangj.dp.model.filter

import scala.collection.mutable

class CptQueryPlan() {
  var udf = "eqUdf"
  val cptFilters = mutable.ArrayBuffer.empty[CptFilter]
  val hashToFilter = new mutable.LinkedHashMap[Int, String]
  val hashToAlias = new mutable.LinkedHashMap[Int, String]
  val aliasToUdfFilter = new mutable.LinkedHashMap[String, mutable.ArrayBuffer[UdfFilter]]
  var counter = 0

  def add(cptFilter: CptFilter): Unit = {
    cptFilter.getCpFilters()
      .flatMap(f => List(f.getNumUdfFilter(), f.getDenUdfFilter()))
      .filter(f => !f.isEmpty)
      .foreach(f => {
        val hash = f.hashCode()

        val alias = getAlias(hash, counter)
        val filter = f.getUdfFilter(udf, alias)


        if (!hashToFilter.contains(hash)) {
          hashToFilter.put(hash, filter)
          hashToAlias.put(hash, alias)
          counter = counter + 1
        }

        addToAlias(alias, f)
      })

    cptFilters += cptFilter
  }

  private def getAlias(hash: Int, counter: Int): String = {
    if (hashToAlias.contains(hash)) {
      hashToAlias.get(hash).get
    } else {
      s"f${counter}"
    }
  }

  private def addToAlias(alias: String, udfFilter: UdfFilter): Unit = {
    if (!aliasToUdfFilter.contains(alias)) {
      aliasToUdfFilter.put(alias, mutable.ArrayBuffer.empty[UdfFilter])
    }

    aliasToUdfFilter.get(alias).get += udfFilter
  }

  def updateCount(alias: String, count: Double): Unit = {
    if (aliasToUdfFilter.contains(alias)) {
      aliasToUdfFilter.get(alias).get.foreach(_.setCount(count))
    }
  }

  def getUdfFilters(): List[String] = {
    hashToFilter.map(_._2).toList
  }

  def setUdf(udf: String): Unit = this.udf = udf

  def getCptFilters(): List[CptFilter] = cptFilters.toList
}
