package com.nlp

import org.scalatest.{FlatSpec, Matchers}


class TryTest extends FlatSpec with Matchers {

    val foo = 2
    "Foo" should "be 2" in {
        foo shouldBe 2
    }

    it should "be more than 1" in {
        foo should be > 1
    }
    "Bar" should "be not like foo" in {
        "Bar" shouldBe foo
    }
}
