package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

class SearchOptionsOperationsSpec
  extends BasicSpec {

  import SearchOptionsOperations._

  private lazy val original = new SearchOptions()

  describe(anInstanceOf[SearchOptionsOperations]) {
    describe(SHOULD) {
      describe("set the following optional values") {
        it("filter") {

          val filterExpression = "filterExpression"
          original.getFilter shouldBe null
          original.setFilter(None).getFilter shouldBe null
          original.setFilter(Some(filterExpression)).getFilter shouldBe filterExpression
        }

        it("select") {

          val select = Seq("f1", "f2")
          original.getSelect shouldBe null
          original.setSelect(None).getSelect shouldBe null
          original.setSelect(Some(select)).getSelect should contain theSameElementsAs select
        }

        it("query type") {

          // TODO: test
        }

        it("facets") {

          // TODO: test
        }
      }
    }
  }
}
