package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, BasicSpec}

class IndexActionTypeGetterSpec
  extends BasicSpec {

  describe(`object`[IndexActionTypeGetter]) {
    describe(SHOULD) {
      describe(s"throw an ${nameOf[AzureSparkException]} for") {
        it("a non existing column") {

          // TODO
        }

        it("a non-string column") {

          // TODO
        }
      }
    }
  }

  describe(anInstanceOf[IndexActionTypeGetter]) {
    describe(SHOULD) {
      describe("retrieve the index action type") {
        it("when not null") {

          // TODO
        }

        it("using a default value when null") {

          // TODO

        }
      }
    }
  }
}
