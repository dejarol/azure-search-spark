package io.github.dejarol.azure.search.spark.connector.core.config

import io.github.dejarol.azure.search.spark.connector.BasicSpec

class SearchIOConfigSpec
  extends BasicSpec {

  private lazy val defaultValue = "v1"
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

          a [ConfigException] shouldBe thrownBy {
            emptyConfig.getEndpoint
          }

          createConfig(
            Map(IOConfig.END_POINT_CONFIG -> defaultValue)
          ).getEndpoint shouldBe defaultValue
        }

        it("the api key") {

          a [ConfigException] shouldBe thrownBy {
            emptyConfig.getAPIkey
          }

          createConfig(
            Map(IOConfig.API_KEY_CONFIG -> defaultValue)
          ).getAPIkey shouldBe defaultValue
        }

        it("the index name") {

          a [ConfigException] shouldBe thrownBy {
            emptyConfig.getIndex
          }

          createConfig(
            Map(IOConfig.INDEX_CONFIG -> defaultValue)
          ).getIndex shouldBe defaultValue

          createConfig(
            Map("path" -> defaultValue)
          ).getIndex shouldBe defaultValue
        }
      }
    }
  }
}
