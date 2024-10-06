package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import org.apache.spark.unsafe.types.UTF8String

class ReadTransformConverterSpec
  extends BasicSpec {

  describe(anInstanceOf[ReadTransformConverter[_]]) {
    describe(SHOULD) {
      describe("apply a transformation to a non-null Search objects, like") {
        it("normal strings") {

          val input = "hello"
          ReadTransformConverter.STRING_VALUE_OF.apply(input) shouldBe input
          ReadTransformConverter.STRING_VALUE_OF.apply(null.asInstanceOf[String]) shouldBe null
        }

        it("UTF8 strings") {

          val input = "hello"
          ReadTransformConverter.UTF8_STRING.apply(input) shouldBe UTF8String.fromString(input)
          ReadTransformConverter.UTF8_STRING.apply(null.asInstanceOf[String]) shouldBe null
        }
      }
    }
  }
}
