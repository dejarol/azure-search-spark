package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{EmptyPartitioner, SinglePartitionPartitioner}

class ReadConfigSpec
  extends BasicSpec {

  private lazy val emptyConfig = ReadConfig(Map.empty[String, String])

  describe(anInstanceOf[ReadConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the filter to apply on index documents") {

          val expected = "filterValue"
          emptyConfig.filter shouldBe empty
          ReadConfig(
            Map(
              ReadConfig.FILTER_CONFIG -> expected
            )
          ).filter shouldBe Some(expected)
        }

        it("the search fields to select") {

          val expected = Seq("f1", "f2")
          emptyConfig.select shouldBe empty
          val actual: Option[Seq[String]] = ReadConfig(
            Map(
              ReadConfig.SELECT_CONFIG -> expected.mkString(",")
            )
          ).select

          actual shouldBe defined
          actual.get should contain theSameElementsAs expected
        }

        it("the partitioner options") {

          val (facet, partitions) = ("facet", 10)
          val partitionerOptions = ReadConfig(
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

            emptyConfig.partitioner shouldBe a[SinglePartitionPartitioner]
          }

          it("a user provided partitioner") {

            val config = ReadConfig(
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
