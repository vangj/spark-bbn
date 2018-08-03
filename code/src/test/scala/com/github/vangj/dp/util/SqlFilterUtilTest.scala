package com.github.vangj.dp.util

import com.github.vangj.dp.model.Variable
import org.scalatest.{FlatSpec, Matchers}

class SqlFilterUtilTest extends FlatSpec with Matchers {
  "one variable" should "generate valid filters" in {
    val v1 = Variable("C0", List("true", "false"))

    val filters = SqlFilterUtil.getFilters(List(v1))
    filters.size should be(2)
    filters.contains("C0='true'") should be(true)
    filters.contains("C0='false'") should be(true)
  }

  "two variables" should "generate valid filters" in {
    val v1 = Variable("C0", List("true", "false"))
    val v2 = Variable("C1", List("1", "0"))

    val filters = SqlFilterUtil.getFilters(List(v1, v2))

    val expected = List(
      "C0='true' and C1='1'",
      "C0='true' and C1='0'",
      "C0='false' and C1='1'",
      "C0='false' and C1='0'")

    filters.size should be(expected.size)
    expected.foreach(e => filters.contains(e) should be(true))
  }

  "three variables" should "generate valid filters" in {
    val v1 = Variable("C0", List("true", "false"))
    val v2 = Variable("C1", List("1", "0"))
    val v3 = Variable("C2", List("a", "b", "c"))

    val filters = SqlFilterUtil.getFilters(List(v1, v2, v3))

    val expected = List(
      "C0='true' and C1='1' and C2='a'",
      "C0='true' and C1='1' and C2='b'",
      "C0='true' and C1='1' and C2='c'",
      "C0='true' and C1='0' and C2='a'",
      "C0='true' and C1='0' and C2='b'",
      "C0='true' and C1='0' and C2='c'",
      "C0='false' and C1='1' and C2='a'",
      "C0='false' and C1='1' and C2='b'",
      "C0='false' and C1='1' and C2='c'",
      "C0='false' and C1='0' and C2='a'",
      "C0='false' and C1='0' and C2='b'",
      "C0='false' and C1='0' and C2='c'"
    )

    filters.size should be(expected.size)
    expected.foreach(e => filters.contains(e) should be(true))
  }

  "three variables in different order" should "generate valid filters" in {
    val v1 = Variable("C0", List("true", "false"))
    val v2 = Variable("C1", List("1", "0"))
    val v3 = Variable("C2", List("a", "b", "c"))

    val filters = SqlFilterUtil.getFilters(List(v3, v2, v1))

    filters.length should be(v1.values.length * v2.values.length * v3.values.length)
  }

  "three variable" should "generate valid ordered filters" in {
    val v1 = Variable("C0", List("true", "false"))
    val v2 = Variable("C1", List("1", "0"))
    val v3 = Variable("C2", List("a", "b", "c"))

    val variables = List(v1, v2, v3)

    val filters = SqlFilterUtil.getOrderedFilters(variables)

    val expected = List(
      "C0='true' and C1='1' and C2='a'",
      "C0='false' and C1='1' and C2='a'",
      "C0='true' and C1='1' and C2='b'",
      "C0='false' and C1='1' and C2='b'",
      "C0='true' and C1='1' and C2='c'",
      "C0='false' and C1='1' and C2='c'",
      "C0='true' and C1='0' and C2='a'",
      "C0='false' and C1='0' and C2='a'",
      "C0='true' and C1='0' and C2='b'",
      "C0='false' and C1='0' and C2='b'",
      "C0='true' and C1='0' and C2='c'",
      "C0='false' and C1='0' and C2='c'"
    )

    filters.size should be(expected.size)
    expected.foreach(e => filters.contains(e) should be(true))
  }

  "conjunctive filters" should "have first element removed" in {
    val v1 = Variable("C0", List("true", "false"))
    val v2 = Variable("C1", List("1", "0"))
    val v3 = Variable("C2", List("a", "b", "c"))

    val variables = List(v1, v2, v3)

    val filters = SqlFilterUtil.trimFilters(SqlFilterUtil.getOrderedFilters(variables))

    val expected = List(
      "C1='1' and C2='a'",
      "C1='1' and C2='a'",
      "C1='1' and C2='b'",
      "C1='1' and C2='b'",
      "C1='1' and C2='c'",
      "C1='1' and C2='c'",
      "C1='0' and C2='a'",
      "C1='0' and C2='a'",
      "C1='0' and C2='b'",
      "C1='0' and C2='b'",
      "C1='0' and C2='c'",
      "C1='0' and C2='c'"
    )

    filters.size should be(expected.size)
    expected.foreach(e => filters.contains(e) should be(true))
  }

  "conditional probability filters" should "be correct" in {
    val v1 = Variable("C0", List("true", "false"))
    val v2 = Variable("C1", List("1", "0"))
    val v3 = Variable("C2", List("a", "b", "c"))

    val variables = List(v1, v2, v3)
    val filters = SqlFilterUtil.getCondProbFilters(variables)

    filters.length should be(variables.foldLeft(1)((a, b) => a * b.values.length))

    val expectedN = List(
      "C0='true' and C1='1' and C2='a'",
      "C0='false' and C1='1' and C2='a'",
      "C0='true' and C1='1' and C2='b'",
      "C0='false' and C1='1' and C2='b'",
      "C0='true' and C1='1' and C2='c'",
      "C0='false' and C1='1' and C2='c'",
      "C0='true' and C1='0' and C2='a'",
      "C0='false' and C1='0' and C2='a'",
      "C0='true' and C1='0' and C2='b'",
      "C0='false' and C1='0' and C2='b'",
      "C0='true' and C1='0' and C2='c'",
      "C0='false' and C1='0' and C2='c'"
    )

    val expectedD = List(
      "C1='1' and C2='a'",
      "C1='1' and C2='a'",
      "C1='1' and C2='b'",
      "C1='1' and C2='b'",
      "C1='1' and C2='c'",
      "C1='1' and C2='c'",
      "C1='0' and C2='a'",
      "C1='0' and C2='a'",
      "C1='0' and C2='b'",
      "C1='0' and C2='b'",
      "C1='0' and C2='c'",
      "C1='0' and C2='c'"
    )

    Seq.range(0, filters.length)
      .foreach(i => {
        filters(i).numerator should be(expectedN(i))
        filters(i).denominator should be(expectedD(i))
      })
  }

  "single cross product" should "work" in {
    val x1 = List("a", "b")

    val actual = SqlFilterUtil.getProduct(List(x1))
    val expected = List("a", "b")

    actual.length should equal(expected.length)

    for (i <- 0 until actual.length) {
      actual(0).mkString(",") should equal(expected(0))
    }
  }

  "triple cross product" should "work" in {
    val x1 = List("a", "b")
    val x2 = List("t", "f")
    val x3 = List("x", "y")

    val actual = SqlFilterUtil.getProduct(List(x1, x2, x3))
    val expected = List("a,t,x", "a,t,y", "a,f,x", "a,f,y", "b,t,x", "b,t,y", "b,f,x", "b,f,y")

    actual.length should equal(expected.length)

    for (i <- 0 until actual.length) {
      actual(0).mkString(",") should equal(expected(0))
    }
  }

  "single variable cpt query plan" should "work" in {
    val v1 = Variable("a", List("t", "f"))
    val qp = SqlFilterUtil.getCptQueryPlan(List(v1))

    val expected = List(
      "eqUdf(array(a),array('t')) as f0",
      "eqUdf(array(a),array('f')) as f1")
    val observed = qp.getUdfFilters()

    observed.length should be(expected.length)
    Seq.range(0, expected.length).foreach(index => observed(index) should equal(expected(index)))
  }

  "two variable cpt query plan" should "work" in {
    val v1 = Variable("a", List("t", "f"))
    val v2 = Variable("b", List("t", "f"))
    val qp = SqlFilterUtil.getCptQueryPlan(List(v1, v2))

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
}
