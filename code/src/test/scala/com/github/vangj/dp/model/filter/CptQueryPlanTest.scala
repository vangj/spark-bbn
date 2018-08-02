package com.github.vangj.dp.model.filter

import org.scalatest.{FlatSpec, Matchers}

class CptQueryPlanTest extends FlatSpec with Matchers {
  "simple query plan" should "generate correct udf filters" in {
    val qp = new CptQueryPlan()
    qp.add(getCptf1())

    val expected = List(
      "eqUdf(array(a,b),array('t','t')) as f0",
      "eqUdf(array(b),array('t')) as f1",
      "eqUdf(array(a,b),array('t','f')) as f2",
      "eqUdf(array(b),array('f')) as f3",
      "eqUdf(array(a,b),array('f','t')) as f4",
      "eqUdf(array(a,b),array('f','f')) as f5")
    val observed = qp.getUdfFilters()

    observed.length should be(expected.length)
    Seq.range(0, expected.length).foreach(index => observed(index) should equal(expected(index)))
  }

  "query plan with 2 cpts" should "generate correct udf filters" in {
    val qp = new CptQueryPlan()
    qp.add(getCptf1())
    qp.add(getCptf2())

    val expected = List(
      "eqUdf(array(a,b),array('t','t')) as f0",
      "eqUdf(array(b),array('t')) as f1",
      "eqUdf(array(a,b),array('t','f')) as f2",
      "eqUdf(array(b),array('f')) as f3",
      "eqUdf(array(a,b),array('f','t')) as f4",
      "eqUdf(array(a,b),array('f','f')) as f5",
      "eqUdf(array(c,d),array('t','t')) as f6",
      "eqUdf(array(d),array('t')) as f7",
      "eqUdf(array(c,d),array('t','f')) as f8",
      "eqUdf(array(d),array('f')) as f9",
      "eqUdf(array(c,d),array('f','t')) as f10",
      "eqUdf(array(c,d),array('f','f')) as f11")
    val observed = qp.getUdfFilters()

    observed.length should be(expected.length)
    Seq.range(0, expected.length).foreach(index => observed(index) should equal(expected(index)))
  }

  def getCptf1(): CptFilter = {
    val udf1 = new UdfFilter(List("a","b"), List("t", "t"))
    val udf2 = new UdfFilter(List("b"), List("t"))
    val udf3 = new UdfFilter(List("a","b"), List("t", "f"))
    val udf4 = new UdfFilter(List("b"), List("f"))
    val udf5 = new UdfFilter(List("a","b"), List("f", "t"))
    val udf6 = new UdfFilter(List("b"), List("t"))
    val udf7 = new UdfFilter(List("a","b"), List("f", "f"))
    val udf8 = new UdfFilter(List("b"), List("f"))

    val cpf1 = new CpFilter(udf1, udf2)
    val cpf2 = new CpFilter(udf3, udf4)
    val cpf3 = new CpFilter(udf5, udf6)
    val cpf4 = new CpFilter(udf7, udf8)

    new CptFilter(List(cpf1, cpf2, cpf3, cpf4))
  }

  private def getCptf2(): CptFilter = {
    val udf1 = new UdfFilter(List("c","d"), List("t", "t"))
    val udf2 = new UdfFilter(List("d"), List("t"))
    val udf3 = new UdfFilter(List("c","d"), List("t", "f"))
    val udf4 = new UdfFilter(List("d"), List("f"))
    val udf5 = new UdfFilter(List("c","d"), List("f", "t"))
    val udf6 = new UdfFilter(List("d"), List("t"))
    val udf7 = new UdfFilter(List("c","d"), List("f", "f"))
    val udf8 = new UdfFilter(List("d"), List("f"))

    val cpf1 = new CpFilter(udf1, udf2)
    val cpf2 = new CpFilter(udf3, udf4)
    val cpf3 = new CpFilter(udf5, udf6)
    val cpf4 = new CpFilter(udf7, udf8)

    new CptFilter(List(cpf1, cpf2, cpf3, cpf4))
  }
}
