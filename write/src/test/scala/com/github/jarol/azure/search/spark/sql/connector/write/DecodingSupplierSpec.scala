package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{SafeCodecSupplierSpec, SchemaViolationsMixins}

class DecodingSupplierSpec
  extends SafeCodecSupplierSpec
    with SchemaViolationsMixins {

  describe(`object`[DecodingSupplier.type ]) {
    describe(SHOULD) {
      describe("return a Left for") {
        it("a") {
          // TODO
        }
      }

      describe("return a Right for") {
        it("a") {
          // TODO
        }
      }
    }
  }
}
