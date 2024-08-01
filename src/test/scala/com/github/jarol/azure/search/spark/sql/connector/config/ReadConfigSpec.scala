package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.config.read.partitioning.EmptyPartitioner
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SinglePartitionPartitioner

class ReadConfigSpec
  extends ConfigSpec {

  private lazy val emptyConfig = readConfig(Map.empty)

  describe(anInstanceOf[ReadConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the filter to apply on index documents") {

          val expected = "filterValue"
          emptyConfig.filter shouldBe empty
          readConfig(
            Map(
              ReadConfig.FILTER_CONFIG -> expected
            )
          ).filter shouldBe Some(expected)
        }

        it("the search fields to select") {

          val expected = Seq("f1", "f2")
          emptyConfig.select shouldBe empty
          val actual: Option[Seq[String]] = readConfig(
            Map(
              ReadConfig.SELECT_CONFIG -> expected.mkString(",")
            )
          ).select

          actual shouldBe defined
          actual.get should contain theSameElementsAs expected
        }

        it("the partitioner options") {

          emptyConfig.partitionerOptions shouldBe empty
          val (facet, partitions) = ("facet", 10)
          val partitionerOptions = readConfig(
            Map(
              ReadConfig.FILTER_CONFIG -> "filter",
              ReadConfig.PARTITIONER_OPTIONS_PREFIX + ReadConfig.PARTITIONER_OPTIONS_FACET_CONFIG -> facet,
              ReadConfig.PARTITIONER_OPTIONS_PREFIX + ReadConfig.PARTITIONER_OPTIONS_FACET_PARTITIONS -> s"$partitions"
            )
          ).partitionerOptions

          partitionerOptions.get(ReadConfig.FILTER_CONFIG) shouldBe empty
          partitionerOptions.get( ReadConfig.PARTITIONER_OPTIONS_FACET_CONFIG) shouldBe defined
          partitionerOptions.get(ReadConfig.PARTITIONER_OPTIONS_FACET_PARTITIONS) shouldBe defined
        }

        describe("a partitioner instance using either") {
          it("a default") {

            emptyConfig.partitioner shouldBe a[SinglePartitionPartitioner]
          }

          it("a user provided partitioner") {

            val config = readConfig(
              Map(
                ReadConfig.PARTITIONER_CONFIG -> classOf[EmptyPartitioner].getName
              )
            )

            config.partitioner shouldBe a [EmptyPartitioner]
          }
        }
      }
    }
  }
}
