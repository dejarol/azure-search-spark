package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

import java.util.function.Supplier

// TODO: convert to integration spec. test only getPartitionResults() and getCountPerPartition
class SearchPartitionSpec
  extends BasicSpec {

  private lazy val nullFilter: Supplier[String] = () => null
  private lazy val emptySelect: Supplier[Seq[String]] = () => Seq.empty

  /**
   * Create an abstract instance of [[SearchPartition]]
   * @param filterSupplier supplier for getting the partition filter
   * @param selectSupplier supplier for getting the selection list
   * @return a Search partition
   */

  private def createAbstractPartition(
                                       filterSupplier: Supplier[String],
                                       selectSupplier: Supplier[Seq[String]]
                                     ): SearchPartition = {

    new SearchPartition {
      override def getPartitionId: Int = 0
      override def getSearchOptions: SearchOptions = {
        new SearchOptions()
          .setFilter(filterSupplier.get())
          .setSelect(selectSupplier.get(): _*)
      }
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
            () => select
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
