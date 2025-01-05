package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class SimpleSearchPartitionSpec
  extends BasicSpec {

  private lazy val filterString = "filterString"
  private lazy val selectionList = Seq("field1", "field2")

  /**
   * Create a partition instance
   * @param maybeFilter optional filter
   * @param maybeSelect optional selection fields
   * @return a partition instance
   */

  private def createPartition(
                               maybeFilter: Option[String],
                               maybeSelect: Option[Seq[String]]
                             ): SimpleSearchPartition = {

    SimpleSearchPartition(
      0,
      maybeFilter,
      maybeSelect,
      Array.empty
    )
  }

  /**
   * Create a partition instance and get its related [[SearchOptions]]
   * @param maybeFilter optional filter
   * @param maybeSelect optional selection fields
   * @return search options
   */

  private def searchOptions(
                             maybeFilter: Option[String],
                             maybeSelect: Option[Seq[String]]
                           ): SearchOptions = {

    createPartition(
      maybeFilter,
      maybeSelect
    ).getSearchOptions
  }

  describe(anInstanceOf[SimpleSearchPartition]) {
    describe(SHOULD) {
      describe("return its inner filter") {
        it("as a null value when not provided") {

          createPartition(None, None).getODataFilter shouldBe null
        }

        it("as a non-null value when defined") {

          createPartition(Some(filterString), None).getODataFilter shouldBe filterString
        }
      }

      describe("create the search options") {
        it("injecting a filter, if defined") {

          searchOptions(None, None).getFilter shouldBe null
          searchOptions(Some(filterString), None).getFilter shouldBe filterString
        }

        it("injecting a selection list, if defined") {

         searchOptions(None, None).getSelect shouldBe null
         searchOptions(None, Some(selectionList)).getSelect should contain theSameElementsAs selectionList
        }
      }
    }
  }
}
