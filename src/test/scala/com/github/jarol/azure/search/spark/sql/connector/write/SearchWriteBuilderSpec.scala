package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.SparkSpec
import com.github.jarol.azure.search.spark.sql.connector.config.ConfigException

class SearchWriteBuilderSpec
  extends SparkSpec {

  describe(`object`[SearchWriteBuilder]) {
    describe(SHOULD) {
      describe("evaluate index action column returning") {
        it("an empty Option for valid columns") {

          // TODO

        }

        it(s"a ${nameOf[ConfigException]} otherwise") {

          // TODO
        }
      }
    }
  }
}