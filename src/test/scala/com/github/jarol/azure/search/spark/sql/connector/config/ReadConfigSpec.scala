package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.config.read.partitioning.EmptyPartitioner
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SinglePartitionPartitioner

class ReadConfigSpec
  extends BasicSpec {

  private lazy val emptyConfig = ReadConfig(Map.empty, Map.empty)

  /**
   * Create a [[ReadConfig]] with given options
   * @param options options passed to the [[org.apache.spark.sql.DataFrameReader]]
   * @return an instance of [[ReadConfig]]
   */

  private def createConfig(options: Map[String, String]): ReadConfig = ReadConfig(options, Map.empty)

  describe(anInstanceOf[ReadConfig]) {
    describe(SHOULD) {
      describe("optionally retrieve") {
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
      }

      describe("retrieve") {
        describe("a partitioner instance using either") {
          it("a default") {

            emptyConfig.partitioner shouldBe a[SinglePartitionPartitioner]
          }

          it("a user provided partitioner") {

            val config = createConfig(
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
