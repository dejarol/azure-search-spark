package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.BasicSpec

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class SearchConfigSpec
  extends BasicSpec {

  private lazy val (k1, v1, k2, v2, k3, v3) = ("k1", "v1", "k2", "v2", "k3", "v3")
  private lazy val keyPrefix = "some.key.prefix."

  /**
   * Create an instance of [[SearchConfig]] by injecting two maps
   * @param m1 local options
   * @param m2 global (SparkConf) options
   * @return an instance of [[SearchConfig]]
   */

  private def createConfig(m1: Map[String, String], m2: Map[String, String]): SearchConfig = new SearchConfig(m1, m2)

  describe(`object`[SearchConfig]) {
    describe(SHOULD) {
      it("retrieve all configs starting with a prefix") {

        SearchConfig.allWithPrefix[Int](
          Map(
            keyPrefix + k1 -> 1,
            keyPrefix + k2 -> 2,
            k3 -> 3
          ),
          keyPrefix
        ) should contain theSameElementsAs Map(
          k1 -> 1,
          k2 -> 2
        )
      }

      describe(s"convert a raw value") {
        it("either successfully") {

          SearchConfig.unsafelyConvert[Double](
            k1,
            "3.14",
            java.lang.Double.parseDouble
          ) shouldBe 3.14
        }

        it(s"or throwing a ${nameOf[ConfigException]}") {

          (the[ConfigException] thrownBy {
            SearchConfig.unsafelyConvert[LocalDate](
              k1,
              v1,
              LocalDate.parse
            )
          }).getMessage should startWith(ConfigException.INVALID_VALUE_PREFIX)
        }
      }
    }
  }

  describe(anInstanceOf[SearchConfig]) {
    describe(SHOULD) {
      describe("get a value") {

        val map = Map(k1 -> v1)
        val emptyConfig = createConfig(Map.empty, Map.empty)
        val configWithOptions = createConfig(map, Map.empty)
        val configWithSparkOptions = createConfig(Map.empty, map)

        it("safely") {

          emptyConfig.get(k1) shouldBe empty
          configWithOptions.get(k1) shouldBe Some(v1)
          configWithSparkOptions.get(k1) shouldBe Some(v1)
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

      describe("get a typed value") {
        it("safely") {

          val (now, formatter) = (LocalDate.now(), DateTimeFormatter.ISO_LOCAL_DATE)
          val config = createConfig(
            Map(
              k1 -> now.format(formatter),
              k3 -> v3
            ),
            Map.empty
          )

          val conversion: String => LocalDate = s => LocalDate.parse(s, formatter)
          config.getAs[LocalDate](k2, conversion) shouldBe empty
          config.getAs[LocalDate](k1, conversion) shouldBe Some(now)
        }

        it("or a default") {

          val default = 1
          val config = createConfig(
            Map(k1 -> v1),
            Map.empty
          )

          config.get(k2) shouldBe empty
          config.getOrDefaultAs[Int](k2, default, _.toInt) shouldBe default
        }

        it("converting the actual value if present") {

          val (existingValue, default) = (123, 1)
          val config = createConfig(
            Map(k1 -> String.valueOf(existingValue)),
            Map.empty
          )

          config.get(k1) shouldBe defined
          config.getOrDefaultAs[Int](k1, default, _.toInt) shouldBe existingValue
        }

        it("eventually throwing an exception for invalid configs") {

          val conversion: String => LocalDate = LocalDate.parse
          val config = createConfig(
            Map(k1 -> "hello"),
            Map.empty
          )

          config.get(k1) shouldBe defined
          (the[ConfigException] thrownBy {
            config.getAs[LocalDate](k1, conversion)
          }).getMessage should startWith(ConfigException.INVALID_VALUE_PREFIX)

          (the [ConfigException] thrownBy {
            config.getOrDefaultAs[LocalDate](k1, LocalDate.now(), conversion)
          }).getMessage should startWith(ConfigException.INVALID_VALUE_PREFIX)
        }
      }

      it("collect all options starting with a prefix in a new config") {

        val config = createConfig(
          Map(
            keyPrefix + k1 -> v1
          ),
          Map(
            keyPrefix + k2 -> v2
          )
        )

        val subConfig = config.getAllWithPrefix(keyPrefix)
        subConfig.unsafelyGet(k1) shouldBe v1
        subConfig.unsafelyGet(k2) shouldBe v2
      }
    }
  }
}
