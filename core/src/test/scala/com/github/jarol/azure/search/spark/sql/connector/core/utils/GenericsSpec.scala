package com.github.jarol.azure.search.spark.sql.connector.core.utils

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import org.scalatest.Inspectors

class GenericsSpec
  extends BasicSpec
    with Inspectors {

  private lazy val enumPredicate: (OperationType, String) => Boolean = (v, s) => v.name().equalsIgnoreCase(s)

  describe(`object`[Generics.type ]) {
    describe(SHOULD) {
      describe("retrieve values from an enum") {

        it("safely") {

          forAll(OperationType.values().toSeq) {
            v => Generics.safeValueOfEnum[OperationType](
              v.name(),
              enumPredicate
            ) shouldBe defined
          }

          Generics.safeValueOfEnum[OperationType]("hello", enumPredicate) shouldBe empty
        }

        it("unsafely") {

          forAll(OperationType.values().toSeq) {
            v => Generics.unsafeValueOfEnum[OperationType](
              v.name(),
              enumPredicate
            ) shouldBe v
          }

          a[NoSuchElementException] shouldBe thrownBy {
            Generics.unsafeValueOfEnum[OperationType]("hello", enumPredicate)
          }
        }
      }
    }
  }
}
