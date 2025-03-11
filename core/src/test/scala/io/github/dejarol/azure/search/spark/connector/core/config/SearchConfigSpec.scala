package io.github.dejarol.azure.search.spark.connector.core.config

import io.github.dejarol.azure.search.spark.connector.core.BasicSpec
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class SearchConfigSpec
  extends BasicSpec {

  private lazy val (k1, v1, k2, v2, k3, v3) = ("key1", "value1", "key2", "value2", "key3", "value3")
  private lazy val keyPrefix = "some.key.prefix."

  /**
   * Create an instance of [[SearchConfig]] by injecting a simple map
   * @param options dataSource options
   * @return an instance of [[SearchConfig]]
   */

  private def createCfg(options: Map[String, String]): SearchConfig = new SearchConfig(options)

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

          SearchConfig.convertOrThrow[Double](
            k1,
            "3.14",
            java.lang.Double.parseDouble
          ) shouldBe 3.14
        }

        it(s"or throwing a ${nameOf[ConfigException]}") {

          (the[ConfigException] thrownBy {
            SearchConfig.convertOrThrow[LocalDate](
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
        val emptyConfig = createCfg(Map.empty)
        val nonEmptyConfig = createCfg(map)

        it("safely") {

          emptyConfig.get(k1) shouldBe empty
          nonEmptyConfig.get(k1) shouldBe Some(v1)
          nonEmptyConfig.get(k1.toUpperCase) shouldBe Some(v1)
          nonEmptyConfig.get(k2) shouldBe empty
        }

        it("unsafely") {

          a [ConfigException] shouldBe thrownBy {
            emptyConfig.unsafelyGet(k1, None, None)
          }

          nonEmptyConfig.unsafelyGet(k1, None, None) shouldBe v1
        }

        it("providing a default value") {

          val default = "defaultValue"
          emptyConfig.getOrDefault(k1, default) shouldBe default
          nonEmptyConfig.getOrDefault(k2, default) shouldBe default
        }
      }

      describe("get a typed value") {
        it("safely") {

          val (now, formatter) = (LocalDate.now(), DateTimeFormatter.ISO_LOCAL_DATE)
          val config = createCfg(
            Map(
              k1 -> now.format(formatter),
              k3 -> v3
            )
          )

          val conversion: String => LocalDate = s => LocalDate.parse(s, formatter)
          config.getAs[LocalDate](k2, conversion) shouldBe empty
          config.getAs[LocalDate](k1, conversion) shouldBe Some(now)
        }

        it("or a default") {

          val default = 1
          val config = createCfg(
            Map(k1 -> v1)
          )

          config.get(k2) shouldBe empty
          config.getOrDefaultAs[Int](k2, default, _.toInt) shouldBe default
        }

        it("converting the actual value if present") {

          val (existingValue, default) = (123, 1)
          val config = createCfg(
            Map(k1 -> String.valueOf(existingValue))
          )

          config.get(k1) shouldBe defined
          config.getOrDefaultAs[Int](k1, default, _.toInt) shouldBe existingValue
        }

        it("eventually throwing an exception for invalid configs") {

          val conversion: String => LocalDate = LocalDate.parse
          val config = createCfg(
            Map(k1 -> "hello")
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

        val config = createCfg(
          Map(
            keyPrefix + k1 -> v1,
            keyPrefix + k2 -> v2,
            k3 -> v3
          )
        )

        val subConfig = config.getAllWithPrefix(keyPrefix)
        subConfig.unsafelyGet(k1, None, None) shouldBe v1
        subConfig.unsafelyGet(k2, None, None) shouldBe v2
        subConfig.get(keyPrefix + k3) shouldBe empty
        subConfig.get(k3) shouldBe empty
      }

      it("split a value into a list") {

        createCfg(
          Map(k1 -> "")
        ).getAsList(k1) shouldBe empty

        createCfg(
          Map(k1 -> " ")
        ).getAsList(k1) shouldBe empty

        val values = Seq("hello ", "world")
        val actual = createCfg(
          Map(k1 -> values.mkString(","))
        ).getAsList(k1)

        val expected = values.map(_.trim)
        actual shouldBe defined
        actual.get should contain theSameElementsAs expected
      }
    }
  }
}
