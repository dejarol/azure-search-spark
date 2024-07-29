package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.BasicSpec
import org.apache.spark.SparkConf

class AbstractIOConfigSpec
  extends BasicSpec {

  private lazy val (k1, v1, k2, v2, k3, v3) = ("k1", "v1", "k2", "v2", "k3", "v3")

  /**
   * Create an instance of [[AbstractIOConfig]] by injecting two maps
 *
   * @param m1 local options
   * @param m2 global (SparkConf) options
   * @return an instance of [[AbstractIOConfig]]
   */

  private def createConfig(m1: Map[String, String], m2: Map[String, String]): AbstractIOConfig = {

    new AbstractIOConfig(m1, m2, UsageMode.WRITE)
  }

  /**
   * Create a test [[SparkConf]], retrieve all configs related to a mode, and run assertions on retrieved result
   * @param usageConfigs usage-related configs
   * @param externalConfigs usage-unrelated (or external) configs
   * @param mode usage mode
   */

  private def testExtractedConfigs(usageConfigs: Map[String, String],
                                   externalConfigs: Map[String, String],
                                   mode: UsageMode): Unit = {

    // Create the test conf
    val sparkConf = new SparkConf()
      .setAll(
        usageConfigs.map {
          case (k, v) => (s"${mode.prefix()}$k", v)
        }
      ).setAll(externalConfigs)

    // Retrieve the result
    val actual = AbstractIOConfig.allConfigsForMode(sparkConf, mode)
    actual should have size usageConfigs.size
    actual should contain theSameElementsAs usageConfigs
  }

  describe(`object`[AbstractIOConfig]) {
    describe(SHOULD) {
      it("retrieve all configs related to a usage mode") {

        val usageConfigs = Map(
          IOConfig.END_POINT_CONFIG -> "azureEndpoint",
          IOConfig.API_KEY_CONFIG -> "azureApiKey"
        )

        val externalConfigs = Map(
          k1 -> v1,
          k2 -> v2
        )

        testExtractedConfigs(usageConfigs, externalConfigs, UsageMode.READ)
        testExtractedConfigs(usageConfigs, externalConfigs, UsageMode.WRITE)
      }
    }
  }

  describe(anInstanceOf[AbstractIOConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the azure endpoint") {

        }

        it("the api key") {

        }

        it("the index name") {

        }
      }
    }
  }
}
