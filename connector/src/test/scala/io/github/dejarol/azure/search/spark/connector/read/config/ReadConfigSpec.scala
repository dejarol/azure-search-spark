package io.github.dejarol.azure.search.spark.connector.read.config

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.config.{ConfigException, SearchConfig}
import io.github.dejarol.azure.search.spark.connector.read.filter.{ODataExpressionMixins, ODataExpressions}
import io.github.dejarol.azure.search.spark.connector.read.partitioning.{DefaultPartitioner, EmptyJavaPartitionerFactory, EmptyScalaPartitionerFactory, FacetedPartitionerFactory, PartitionerFactory, RangePartitionerFactory, SinglePartitionFactory}
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.DataTypes

class ReadConfigSpec
  extends BasicSpec
    with ODataExpressionMixins
      with FieldFactory {

  /**
   * Create a configuration instance
   * @param options configuration options
   * @return a configuration instance
   */

  private def createConfig(options: Map[String, String]): ReadConfig = ReadConfig(options)

  // Empty instance for testing
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

  /**
   * Create a configuration instance by setting the given value for key <code>partitioner</code>
   * @param partitionerName the partitioner factory class name
   * @return a configuration instance
   */

  private def createConfigWithPartitionerFactory(partitionerName: String): ReadConfig = {

    createConfig(
      Map(
        ReadConfig.PARTITIONER_CLASS_CONFIG -> partitionerName
      )
    )
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

        describe("a partitioner factory instance of type") {
          it("range") {

            createConfigWithPartitionerFactory(
              ReadConfig.RANGE_PARTITIONER_CLASS_VALUE
            ).partitionerFactory shouldBe a[RangePartitionerFactory]
          }

          it("faceted") {

            createConfigWithPartitionerFactory(
              ReadConfig.FACETED_PARTITIONER_CLASS_VALUE
            ).partitionerFactory shouldBe a[FacetedPartitionerFactory]
          }

          describe("custom") {
            it("using valid Java classes") {

              createConfigWithPartitionerFactory(
               classOf[EmptyJavaPartitionerFactory].getName
              ).partitionerFactory shouldBe a[EmptyJavaPartitionerFactory]
            }

            it("using valid Scala classes") {

              createConfigWithPartitionerFactory(
                classOf[EmptyScalaPartitionerFactory].getName
              ).partitionerFactory shouldBe a[EmptyScalaPartitionerFactory]
            }

            describe(s"throwing a ${nameOf[ConfigException]} for") {
              it("invalid class names") {

                a [ConfigException] shouldBe thrownBy {
                  createConfigWithPartitionerFactory("hello")
                    .partitionerFactory
                }
              }

              it(s"class names not implementing ${nameOf[PartitionerFactory]}") {

                a [ConfigException] shouldBe thrownBy {
                  createConfigWithPartitionerFactory(
                    classOf[String].getName
                  ).partitionerFactory
                }
              }

              it("partitioner factories without a no-arg constructor") {

                a [ConfigException] shouldBe thrownBy {
                  createConfigWithPartitionerFactory(
                    classOf[SinglePartitionFactory].getName
                  ).partitionerFactory
                }
              }
            }
          }

          it("default") {

            val factory = emptyConfig.partitionerFactory
            factory.createPartitioner(emptyConfig) shouldBe a [DefaultPartitioner]
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

        describe("upsert") {
          it("pushed predicates") {

            // If no predicates were provided, we expect the pushed predicate to be empty
            val idIsNotNull = ODataExpressions.isNull(topLevelFieldReference("id"), negate = true)
            val descriptionIsNotNull = ODataExpressions.isNull(topLevelFieldReference("description"), negate = true)

            emptyConfig
              .withPushedPredicates(Seq.empty)
              .searchOptionsBuilderConfig.pushedPredicate shouldBe empty

            // If only predicate was provided, that should be pushed
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

          it("select clause") {

            // An empty schema is provided, we do not expect the select clause
            emptyConfig
              .withSelectClause(
                createStructType()
              ).searchOptionsBuilderConfig.select shouldBe empty

            val fields = Seq(
              createStructField("id", DataTypes.StringType),
              createStructField("value", DataTypes.IntegerType)
            )

            val maybeResult = emptyConfig
              .withSelectClause(
                createStructType(
                  fields: _*
                )
              ).searchOptionsBuilderConfig.select

            maybeResult shouldBe defined
            maybeResult.get should contain theSameElementsAs fields.map(_.name)
          }

          it("index name") {

            // For an empty configuration, a ConfigException should be thrown
            a [ConfigException] shouldBe thrownBy {
              emptyConfig.getIndex
            }

            emptyConfig
              .withIndexName("hello")
              .getIndex shouldBe "hello"
          }

          it("new options (case insensitive)") {

            val (k1, v1, v2) = ("keyOne", "v1", "v2")
            val firstOptions = Map(k1 -> v1)
            val secondOptions = Map(k1.toUpperCase -> v2)

            // No options, the result should be empty
            emptyConfig.get(k1) shouldBe empty

            // We expect to retrieve value 'v1'
            val updatedConfig = emptyConfig.withOptions(
              JavaScalaConverters.scalaMapToJava(firstOptions)
            )

            updatedConfig.get(k1) shouldBe Some(v1)

            // We expect to retrieve value 'v2'
            // (the value should have been updated event though the key is different, case-wise)
            updatedConfig.withOptions(
              JavaScalaConverters.scalaMapToJava(secondOptions)
            ).get(k1) shouldBe Some(v2)
          }
        }
      }
    }
  }
}
