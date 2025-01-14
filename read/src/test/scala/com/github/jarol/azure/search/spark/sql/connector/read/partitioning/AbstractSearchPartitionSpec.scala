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
                                           inputFilter: Option[String],
                                           partFilter: Option[String]
                                         ): String = {

    // TODO: fix
    new AbstractSearchPartition(
      0, null
    ) {
      override protected[partitioning] def partitionFilter: Option[String] = partFilter
    }.getSearchOptions.getFilter
  }

  describe(anInstanceOf[AbstractSearchPartition]) {
    describe(SHOULD) {
      describe("create a filter that") {
        it("combines partition filter with the supplier filter") {

          val (first, second) = ("a eq 1", "b eq 2")

          // Assert behavior
          createPartitionAndGetFilter(None, None) shouldBe null
          createPartitionAndGetFilter(Some(first), None) shouldBe first
          createPartitionAndGetFilter(None, Some(first)) shouldBe first

          // Assert behavior when combining
          createPartitionAndGetFilter(Some(first), Some(second)) shouldBe s"($first) and ($second)"
        }
      }
    }
  }

  describe(`object`[AbstractSearchPartition]) {
    describe(SHOULD) {
      it("create a combined OData filter") {

        val (first, second) = ("a eq 1", "b eq 2")
        AbstractSearchPartition.createODataFilter(Seq.empty) shouldBe empty
        AbstractSearchPartition.createODataFilter(Seq(first)) shouldBe Some(first)
        AbstractSearchPartition.createODataFilter(Seq(first, second)) shouldBe Some(s"($first) and ($second)")
      }
    }
  }
}
