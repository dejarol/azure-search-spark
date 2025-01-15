package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class AbstractSearchPartitionSpec
  extends BasicSpec {

  /**
   * Create a partition instance
   * @param inputFilter input (user-specified) filter
   * @param partFilter partition filter
   * @return a partition instance
   */

  private def createPartitionAndGetFilter(
                                           inputFilter: String,
                                           partFilter: Option[String]
                                         ): String = {

    new AbstractSearchPartition(
      0,
      SimpleOptionsBuilder(
        Seq(inputFilter),
        Seq.empty
      )
    ) {
      override protected[partitioning] def partitionFilter: Option[String] = partFilter
    }.getSearchOptions.getFilter
  }

  describe(anInstanceOf[AbstractSearchPartition]) {
    describe(SHOULD) {
      describe("create a filter that") {
        it("combines partition filter with the builder filter") {

          val (first, second) = ("a eq 1", "b eq 2")
          createPartitionAndGetFilter(first, None) shouldBe first
          createPartitionAndGetFilter(first, Some(second)) shouldBe s"$first,$second"
        }
      }
    }
  }
}