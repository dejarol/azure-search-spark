package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.BasicSpec
import org.apache.spark.SparkConf

class AbstractSearchConfigSpec
  extends BasicSpec {

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
    val actual = AbstractSearchConfig.allConfigsForMode(sparkConf, mode)
    actual should have size usageConfigs.size
    actual should contain theSameElementsAs usageConfigs
  }

  describe(`object`[AbstractSearchConfig]) {
    describe(SHOULD) {
      it("retrieve all configs related to a usage mode") {

        val usageConfigs = Map(
          SearchConfig.END_POINT_CONFIG -> "azureEndpoint",
          SearchConfig.API_KEY_CONFIG -> "azureApiKey"
        )

        val externalConfigs = Map(
          "k1" -> "v1",
          "k2" -> "v2"
        )

        testExtractedConfigs(usageConfigs, externalConfigs, UsageMode.READ)
        testExtractedConfigs(usageConfigs, externalConfigs, UsageMode.WRITE)
      }
    }
  }
}
