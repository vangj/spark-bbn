package com.github.vangj.dp.model

import org.scalatest.{FlatSpec, Matchers}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

class VariableProfileTest extends FlatSpec with Matchers {

  "variable profile" should "serialize to JSON properly" in {
    val s1 = new VariableProfile(0, "a", List(new ValueProfile("true", 100L), new ValueProfile("false", 200L)))
    val s2 = Source.fromFile("src/test/resources/data/variable-profile-test-single.json").mkString.trim

    pretty(s1.json()) should equal(s2)
  }

  "list of variable profiles" should "serialize to JSON properly" in {
    val profiles = List(
      new VariableProfile(0, "a", List(new ValueProfile("true", 100L), new ValueProfile("false", 200L))),
      new VariableProfile(1, "b", List(new ValueProfile("male", 1050L), new ValueProfile("female", 2300L))),
      new VariableProfile(2, "c", List(new ValueProfile("on", 1020L), new ValueProfile("off", 2200L)))
    )

    val s1 = pretty(profiles.map(_.json()))
    val s2 = Source.fromFile("src/test/resources/data/variable-profile-test-multiple.json").mkString.trim

    s1 should equal(s2)
  }

}
