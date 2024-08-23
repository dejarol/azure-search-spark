package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.BasicSpec
import org.apache.spark.SparkConf

class SearchIOConfigSpec
  extends BasicSpec {

  private lazy val (k1, v1, k2, v2, k3, v3) = ("k1", "v1", "k2", "v2", "k3", "v3")

  /**
   * Create an instance of [[SearchIOConfig]] by injecting two maps
 *
   * @param m1 local options
   * @param m2 global (SparkConf) options
   * @return an instance of [[SearchIOConfig]]
   */

  private def createConfig(m1: Map[String, String], m2: Map[String, String]): SearchIOConfig = new SearchIOConfig(m1, m2)

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
    val actual = SearchIOConfig.allConfigsForMode(sparkConf, mode)
    actual should have size usageConfigs.size
    actual should contain theSameElementsAs usageConfigs
  }

  describe(`object`[SearchIOConfig]) {
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

  describe(anInstanceOf[SearchIOConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the azure endpoint") {

          createConfig(
            Map(IOConfig.END_POINT_CONFIG -> v1),
            Map.empty
          ).getEndpoint shouldBe v1

          createConfig(
            Map.empty,
            Map(IOConfig.END_POINT_CONFIG -> v2),
          ).getEndpoint shouldBe v2

          createConfig(
            Map(IOConfig.END_POINT_CONFIG -> v3),
            Map(IOConfig.END_POINT_CONFIG -> v2),
          ).getEndpoint shouldBe v3
        }

        it("the api key") {

          createConfig(
            Map(IOConfig.API_KEY_CONFIG -> v1),
            Map.empty
          ).getEndpoint shouldBe v1

          createConfig(
            Map.empty,
            Map(IOConfig.API_KEY_CONFIG -> v2),
          ).getEndpoint shouldBe v2

          createConfig(
            Map(IOConfig.API_KEY_CONFIG -> v3),
            Map(IOConfig.API_KEY_CONFIG -> v2),
          ).getEndpoint shouldBe v3
        }

        it("the index name") {

          createConfig(
            Map(IOConfig.INDEX_CONFIG -> v1),
            Map.empty
          ).getEndpoint shouldBe v1

          createConfig(
            Map.empty,
            Map(IOConfig.INDEX_CONFIG -> v2),
          ).getEndpoint shouldBe v2

          createConfig(
            Map(IOConfig.INDEX_CONFIG -> v3),
            Map(IOConfig.INDEX_CONFIG -> v2),
          ).getEndpoint shouldBe v3
        }
      }
    }
  }
}
