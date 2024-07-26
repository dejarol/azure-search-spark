package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.BasicSpec
import org.apache.spark.SparkConf

import java.time.LocalDate

class AbstractSearchConfigSpec
  extends BasicSpec {

  private lazy val (k1, v1, k2, v2) = ("k1", "v1", "k2", "v2")

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
          k1 -> v1,
          k2 -> v2
        )

        testExtractedConfigs(usageConfigs, externalConfigs, UsageMode.READ)
        testExtractedConfigs(usageConfigs, externalConfigs, UsageMode.WRITE)
      }
    }
  }

  describe(anInstanceOf[AbstractSearchConfig]) {
    describe(SHOULD) {
      describe("get a value") {

        val map = Map(k1 -> v1)
        val emptyConfig = new AbstractSearchConfig(Map.empty, Map.empty, UsageMode.READ) {}
        val configWithOptions = new AbstractSearchConfig(map, Map.empty, UsageMode.READ) {}
        val configWithSparkOptions = new AbstractSearchConfig(Map.empty, map, UsageMode.READ) {}

        it("safely") {

          emptyConfig.safelyGet(k1) shouldBe empty
          configWithOptions.safelyGet(k1) shouldBe Some(v1)
          configWithSparkOptions.safelyGet(k1) shouldBe Some(v1)
        }

        it("unsafely") {

          (the [ConfigException] thrownBy {
            emptyConfig.unsafelyGet(k1)
          }).getMessage should startWith (ConfigException.MISSING_REQUIRED_OPTION_PREFIX)

          configWithOptions.unsafelyGet(k1) shouldBe v1
          configWithSparkOptions.unsafelyGet(k1) shouldBe v1
        }

        it("providing a default value") {

          val default = "defaultValue"
          emptyConfig.getOrDefault(k1, default) shouldBe default
          configWithOptions.getOrDefault(k2, default) shouldBe default
          configWithSparkOptions.getOrDefault(k2, default) shouldBe default
        }
      }

      describe("retrieve a typed value") {
        it("providing a default when a key is missing") {

          val default = 1
          val config = new AbstractSearchConfig(
            Map(k1 -> v1),
            Map.empty,
            UsageMode.WRITE
          ) {}

          config.safelyGet(k2) shouldBe empty
          config.getAsOrDefault[Int](k2, default, _.toInt) shouldBe default
        }

        it("converting the actual value if present") {

          val (existingValue, default) = (123, 1)
          val config = new AbstractSearchConfig(
            Map(k1 -> String.valueOf(existingValue)),
            Map.empty,
            UsageMode.WRITE
          ) {}

          config.safelyGet(k1) shouldBe defined
          config.getAsOrDefault[Int](k1, default, _.toInt) shouldBe existingValue
        }

        it("eventually throwing an exception for invalid configs") {

          val config = new AbstractSearchConfig(
            Map(k1 -> "hello"),
            Map.empty,
            UsageMode.WRITE
          ) {}

          config.safelyGet(k1) shouldBe defined
          (the [ConfigException] thrownBy {
            config.getAsOrDefault[LocalDate](k1, LocalDate.now(), LocalDate.parse)
          }).getMessage should startWith(ConfigException.INVALID_VALUE_PREFIX)
        }
      }
    }
  }
}
