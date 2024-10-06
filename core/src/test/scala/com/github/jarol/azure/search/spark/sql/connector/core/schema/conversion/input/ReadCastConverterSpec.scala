package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class ReadCastConverterSpec
  extends BasicSpec {

  describe(anInstanceOf[ReadCastConverter[_]]) {
    describe(SHOULD) {
      describe("cast a non-null Search object into a Spark internal type, like") {
        it("int") {

          val input: Integer = 23
          ReadCastConverter.INT32.apply(input) shouldBe input
          ReadCastConverter.INT32.apply(null.asInstanceOf[Integer]) shouldBe null
        }

        it("long") {

          val input: java.lang.Long = 23
          ReadCastConverter.INT64.apply(input) shouldBe input
          ReadCastConverter.INT64.apply(null.asInstanceOf[java.lang.Long]) shouldBe null
        }

        it("double") {

          val input: java.lang.Double = 3.14
          ReadCastConverter.DOUBLE.apply(input) shouldBe input
          ReadCastConverter.DOUBLE.apply(null.asInstanceOf[java.lang.Long]) shouldBe null
        }

        it("float") {

          val input: java.lang.Float = 3.14f
          ReadCastConverter.SINGLE.apply(input) shouldBe input
          ReadCastConverter.SINGLE.apply(null.asInstanceOf[java.lang.Long]) shouldBe null
        }

        it("boolean") {

          val input: java.lang.Boolean = false
          ReadCastConverter.BOOLEAN.apply(input) shouldBe input
          ReadCastConverter.BOOLEAN.apply(null.asInstanceOf[java.lang.Boolean]) shouldBe null
        }
      }
    }
  }
}
