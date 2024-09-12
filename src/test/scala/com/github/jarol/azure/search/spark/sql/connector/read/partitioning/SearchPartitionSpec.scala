package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, JavaScalaConverters}

import java.util
import java.util.function.Supplier

class SearchPartitionSpec
  extends BasicSpec {

  private lazy val nullFilter: Supplier[String] = () => null
  private lazy val emptySelect: Supplier[util.List[String]] = () => util.Collections.emptyList()

  /**
   * Create an abstract instance of [[SearchPartition]]
   * @param filterSupplier supplier for getting the partition filter
   * @param selectSupplier supplier for getting the selection list
   * @return a Search partition
   */

  private def createAbstractPartition(filterSupplier: Supplier[String],
                                      selectSupplier: Supplier[util.List[String]]): SearchPartition = {

    new SearchPartition {
      override def getPartitionId: Int = 0
      override def getSearchFilter: String = filterSupplier.get()
      override def getSearchSelect: util.List[String] = selectSupplier.get()
    }
  }

  describe(anInstanceOf[SearchPartition]) {
    describe(SHOULD) {
      describe("setup search options") {
        it("adding a filter when defined") {

          val filter = "filterString"
          createAbstractPartition(
            () => filter,
            emptySelect
          ).getSearchOptions.getFilter shouldBe filter
        }

        it("leaving the filter unset when not defined") {

          createAbstractPartition(
            nullFilter,
            emptySelect
          ).getSearchOptions.getFilter shouldBe null
        }

        it("adding a selection list when defined") {

          val select = Seq("first", "second")
          createAbstractPartition(
            nullFilter,
            () => JavaScalaConverters.seqToList(select)
          ).getSearchOptions.getSelect should contain theSameElementsAs select
        }

        describe("leaving the selection list unset when") {
          it("is null") {

            createAbstractPartition(
              nullFilter,
              () => null
            ).getSearchOptions.getSelect shouldBe null
          }

          it("is empty") {

            createAbstractPartition(
              nullFilter,
              emptySelect
            ).getSearchOptions.getSelect shouldBe null
          }
        }
      }
    }
  }
}
