package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, JavaScalaConverters}
import org.apache.spark.sql.connector.expressions.filter.Predicate

import java.util.{Collections => JCollections, List => JList}
import java.util.function.Supplier

class SearchPartitionSpec
  extends BasicSpec {

  private lazy val nullFilter: Supplier[String] = () => null
  private lazy val emptySelect: Supplier[JList[String]] = () => JCollections.emptyList()

  /**
   * Create an abstract instance of [[SearchPartition]]
   * @param filterSupplier supplier for getting the partition filter
   * @param selectSupplier supplier for getting the selection list
   * @return a Search partition
   */

  private def createAbstractPartition(filterSupplier: Supplier[String],
                                      selectSupplier: Supplier[JList[String]]): SearchPartition = {

    new SearchPartition {
      override def getPartitionId: Int = 0
      override def getODataFilter: String = filterSupplier.get()
      override def getSelectedFields: JList[String] = selectSupplier.get()
      override def getPushedPredicates: Array[Predicate] = Array.empty
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
