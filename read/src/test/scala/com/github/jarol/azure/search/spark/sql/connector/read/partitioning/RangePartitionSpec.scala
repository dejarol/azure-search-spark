package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class RangePartitionSpec
  extends BasicSpec {

  private lazy val fieldName = "field"

  /**
   * Create a partition instance
   * @param inputFilter input filter
   * @param lowerBound lower bound
   * @param upperBound upper bound
   * @return
   */

  private def getSearchFilter(
                               inputFilter: Option[String],
                               lowerBound: Option[String],
                               upperBound: Option[String]
                             ): String = {

    RangePartition(
      0,
      inputFilter,
      None,
      Seq.empty,
      fieldName,
      lowerBound,
      upperBound
    ).getODataFilter
  }

  describe(anInstanceOf[RangePartition]) {
    describe(SHOULD) {
      it("generate a filter that combines the 3 sub filters") {

        val (inputFilter, lb, ub) = ("name eq 'hello'", "1", "3")
        getSearchFilter(None, None, None) shouldBe null
        getSearchFilter(Some(inputFilter), None, None) shouldBe inputFilter
        val secondFilter = getSearchFilter(Some(inputFilter), Some(lb), None)
        secondFilter should include (inputFilter)
        secondFilter should include (s"$fieldName ge $lb")

        val thirdFilter = getSearchFilter(None, None, Some(ub))
        thirdFilter should include (s"$fieldName lt $ub")
        thirdFilter should include (s"$fieldName eq null")

        val fourthFilter = getSearchFilter(Some(inputFilter), Some(lb), Some(ub))
        fourthFilter should include (inputFilter)
        fourthFilter should include (s"$fieldName ge $lb")
        fourthFilter should include (s"$fieldName lt $ub")
      }
    }
  }

  describe(`object`[RangePartition]) {
    describe(SHOULD) {
      it("create a collection of partitions") {

        val values = Seq("1", "2", "3")
        val partitions = RangePartition.createCollection(None, None, Seq.empty, fieldName, values)
        partitions should have size(values.size + 1)
        val headFilter = partitions.head.getODataFilter
        headFilter should include (s"$fieldName lt ${values.head}")
        headFilter should include (s"$fieldName eq null")
        partitions.last.getODataFilter shouldBe s"$fieldName ge ${values.last}"
      }
    }
  }
}
