package io.github.jarol.azure.search.spark.connector.core.utils

import io.github.jarol.azure.search.spark.connector.core.BasicSpec

class EnumsSpec
  extends BasicSpec {

  private lazy val enumPredicate: (OperationType, String) => Boolean = (v, s) => v.name().equalsIgnoreCase(s)

  describe(`object`[Enums.type ]) {
    describe(SHOULD) {
      describe("retrieve values from an enum") {

        it("safely") {

          forAll(OperationType.values().toSeq) {
            v => Enums.safeValueOf[OperationType](
              v.name(),
              enumPredicate
            ) shouldBe defined
          }

          Enums.safeValueOf[OperationType]("hello", enumPredicate) shouldBe empty
        }

        it("unsafely") {

          forAll(OperationType.values().toSeq) {
            v => Enums.unsafeValueOf[OperationType](
              v.name(),
              enumPredicate
            ) shouldBe v
          }

          a[NoSuchElementException] shouldBe thrownBy {
            Enums.unsafeValueOf[OperationType]("hello", enumPredicate)
          }
        }
      }
    }
  }
}
