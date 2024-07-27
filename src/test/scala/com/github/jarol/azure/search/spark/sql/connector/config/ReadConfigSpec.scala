package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.BasicSpec

class ReadConfigSpec
  extends BasicSpec {

  private lazy val emptyConfig = ReadConfig(Map.empty, Map.empty)

  /**
   * Create a [[ReadConfig]] with given options
   * @param options options passed to the [[org.apache.spark.sql.DataFrameReader]]
   * @return an instance of [[ReadConfig]]
   */

  private def createConfig(options: Map[String, String]): ReadConfig = ReadConfig(options, Map.empty)

  describe(anInstanceOf[ReadConfig]) {
    describe(SHOULD) {
      describe("optionally retrieve") {
        it("the filter to apply on index documents") {

          val filter = "filterValue"
          emptyConfig.filter shouldBe empty
          createConfig(
            Map(
              ReadConfig.FILTER_CONFIG -> filter
            )
          ).filter shouldBe Some(filter)
        }

        describe("a partitioner instance") {
          it("default") {

            //TODO
          }

          it("user-specified") {
            //TODO
          }
        }
      }
    }
  }
}
