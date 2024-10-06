package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class AtomicReadConvertersSpec
  extends BasicSpec {

  import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.AtomicReadConverters._

  describe("Each atomic type converter") {
    describe(SHOULD) {
      describe("convert a Search object into a Spark internal object") {

        it("int to long") {

          val input = 123
          val actual = Int32ToLongConverter.apply(input)
          actual shouldBe a[java.lang.Long]
          actual shouldBe input.toLong
        }

        it("long to int") {

          val input = 123
          val actual = Int64ToIntConverter.apply(input.toLong)
          actual shouldBe a[java.lang.Integer]
          actual shouldBe input
        }
      }
    }
  }
}
