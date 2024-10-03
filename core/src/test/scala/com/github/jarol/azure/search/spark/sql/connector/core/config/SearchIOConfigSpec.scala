package com.github.jarol.azure.search.spark.sql.connector.core.config

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class SearchIOConfigSpec
  extends BasicSpec {

  private lazy val endpoint = "v1"
  private lazy val emptyConfig = new SearchIOConfig(Map.empty[String, String])

  /**
   * Create an instance of [[SearchIOConfig]] by injecting a simple map
   * @param dsOptions local options
   * @return an instance of [[SearchIOConfig]]
   */

  private def createConfig(dsOptions: Map[String, String]): SearchIOConfig = new SearchIOConfig(dsOptions)

  describe(anInstanceOf[SearchIOConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the azure endpoint") {

          emptyConfig.getEndpoint

          createConfig(
            Map(IOConfig.END_POINT_CONFIG -> endpoint)
          ).getEndpoint shouldBe endpoint
        }

        it("the api key") {

          createConfig(
            Map(IOConfig.API_KEY_CONFIG -> endpoint)
          ).getAPIkey shouldBe endpoint
        }

        it("the index name") {

          createConfig(
            Map(IOConfig.INDEX_CONFIG -> endpoint)
          ).getIndex shouldBe endpoint
        }
      }
    }
  }
}
