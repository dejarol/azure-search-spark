package com.github.jarol.azure.search.spark.sql.connector.read.config

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.config.{ConfigException, SearchConfig}
import com.github.jarol.azure.search.spark.sql.connector.read.filter.{ODataExpressionFactory, ODataExpressions}
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{DefaultPartitioner, EmptyPartitioner}

class ReadConfigSpec
  extends BasicSpec
    with ODataExpressionFactory {

  /**
   * Create a configuration instance
   * @param options configuration options
   * @return a configuration instance
   */

  private def createConfig(options: Map[String, String]): ReadConfig = ReadConfig(options)

  private lazy val emptyConfig = createConfig(Map.empty)

  /**
   * Assert that an inner [[SearchConfig]] instance, obtained by a [[ReadConfig]] (like the one for search or partitioner options)
   * contains the proper set of elements (which should be all key-value pairs whose keys start with a given prefix)
   * @param input configuration map to use for creating the [[ReadConfig]]
   * @param prefix prefix to be matched by the key-value pairs of the inner [[SearchConfig]]
   * @param getter function for getting the [[SearchConfig]] from the [[ReadConfig]]
   */

  private def assertSubConfigurationContains(
                                              input: Map[String, String],
                                              prefix: String,
                                              getter: ReadConfig => SearchConfig
                                            ): Unit = {

    // Empty map should give an empty map
    getter(emptyConfig).toMap shouldBe empty
    val createdConfig: ReadConfig = createConfig(input)

    // Given a non-empty map, we expect to find all of its pairs
    val expected = input.collect {
      case (k, v)  if k.startsWith(prefix) =>
        (k.stripPrefix(prefix), v)
    }

    getter(createdConfig).toMap should contain theSameElementsAs expected
  }

  describe(anInstanceOf[ReadConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        describe("options related to") {
          it("documents search") {

            assertSubConfigurationContains(
              Map(
                ReadConfig.SEARCH_OPTIONS_PREFIX + "k1" -> "v1",
                ReadConfig.SEARCH_OPTIONS_PREFIX + "k2" -> "v2",
                "k3" -> "v3"
              ),
              ReadConfig.SEARCH_OPTIONS_PREFIX,
              _.searchOptionsBuilderConfig
            )
          }

          it("partitioners") {

            assertSubConfigurationContains(
              Map(
                "k1" -> "v1",
                ReadConfig.PARTITIONER_OPTIONS_PREFIX + "k2" -> "v2",
                ReadConfig.PARTITIONER_OPTIONS_PREFIX + "k3" -> "v3"
              ),
              ReadConfig.PARTITIONER_OPTIONS_PREFIX,
              _.partitionerOptions
            )
          }
        }

        describe("a partitioner instance using either") {
          it("a default") {

            emptyConfig.partitionerClass shouldBe classOf[DefaultPartitioner]
          }

          it("a user provided partitioner") {

            val config = createConfig(
              Map(
                ReadConfig.PARTITIONER_CLASS_CONFIG -> classOf[EmptyPartitioner].getName
              )
            )

            config.partitionerClass shouldBe classOf[EmptyPartitioner]
          }
        }

        it("the predicate pushdown flag") {

          // Default
          emptyConfig.pushdownPredicate shouldBe true

          // Expecting false
          createConfig(
            Map(
              ReadConfig.PUSHDOWN_PREDICATE_CONFIG -> "false"
            )
          ).pushdownPredicate shouldBe false

          // Expecting true
          createConfig(
            Map(
              ReadConfig.PUSHDOWN_PREDICATE_CONFIG -> "true"
            )
          ).pushdownPredicate shouldBe true

          // Invalid value
          a [ConfigException] shouldBe thrownBy {

            createConfig(
              Map(
                ReadConfig.PUSHDOWN_PREDICATE_CONFIG -> "hello"
              )
            ).pushdownPredicate
          }
        }

        it("include pushed predicates") {

          // If no predicates were provided, we expect the pushed predicate to be empty
          val idIsNotNull = ODataExpressions.isNull(topLevelFieldReference("id"), negate = true)
          val descriptionIsNotNull = ODataExpressions.isNull(topLevelFieldReference("description"), negate = true)

          emptyConfig
            .withPushedPredicates(Seq.empty)
            .searchOptionsBuilderConfig.pushedPredicate shouldBe empty

          // If only predicate was provided, that should be being pushed
          emptyConfig
            .withPushedPredicates(Seq(idIsNotNull))
            .searchOptionsBuilderConfig.pushedPredicate shouldBe Some(idIsNotNull.toUriLiteral)

          // If many predicates were provided, we expect the pushed predicate to be the AND combination of the original predicates
          val expected = ODataExpressions.logical(
            Seq(idIsNotNull, descriptionIsNotNull),
            isAnd = true
          )

          emptyConfig.withPushedPredicates(
            Seq(idIsNotNull, descriptionIsNotNull)
          ).searchOptionsBuilderConfig.pushedPredicate shouldBe Some(expected.toUriLiteral)
        }
      }
    }
  }
}
