package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.config.ConfigException
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{EmptyPartitioner, SinglePartitionPartitioner}

class ReadConfigSpec
  extends BasicSpec {

  /**
   * Create a configuration instance
   * @param options configuration options
   * @return a configuration instance
   */

  private def createConfig(options: Map[String, String]): ReadConfig = ReadConfig(options)

  private lazy val emptyConfig = createConfig(Map.empty)

  describe(anInstanceOf[ReadConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the filter to apply on index documents") {

          val expected = "filterValue"
          emptyConfig.filter shouldBe empty
          createConfig(
            Map(
              ReadConfig.FILTER_CONFIG -> expected
            )
          ).filter shouldBe Some(expected)
        }

        it("the search fields to select") {

          val expected = Seq("f1", "f2")
          emptyConfig.select shouldBe empty
          val actual: Option[Seq[String]] = createConfig(
            Map(
              ReadConfig.SELECT_CONFIG -> expected.mkString(",")
            )
          ).select

          actual shouldBe defined
          actual.get should contain theSameElementsAs expected
        }

        it("the partitioner options") {

          val (facet, partitions) = ("facet", 10)
          val partitionerOptions = createConfig(
            Map(
              ReadConfig.FILTER_CONFIG -> "filter",
              ReadConfig.PARTITIONER_OPTIONS_PREFIX + ReadConfig.FACET_FIELD_CONFIG -> facet,
              ReadConfig.PARTITIONER_OPTIONS_PREFIX + ReadConfig.NUM_PARTITIONS_CONFIG -> s"$partitions"
            )
          ).partitionerOptions

          partitionerOptions.get(ReadConfig.FILTER_CONFIG) shouldBe empty
          partitionerOptions.get( ReadConfig.FACET_FIELD_CONFIG) shouldBe defined
          partitionerOptions.get(ReadConfig.NUM_PARTITIONS_CONFIG) shouldBe defined
        }

        describe("a partitioner instance using either") {
          it("a default") {

            emptyConfig.partitionerClass shouldBe a[SinglePartitionPartitioner]
          }

          it("a user provided partitioner") {

            val config = createConfig(
              Map(
                ReadConfig.PARTITIONER_CONFIG -> classOf[EmptyPartitioner].getName
              )
            )

            config.partitionerClass shouldBe a [EmptyPartitioner]
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
      }
    }
  }
}
