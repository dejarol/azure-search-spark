package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.models.{QueryType, SearchMode, SearchOptions}
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

          val queryType = QueryType.FULL
          original.getQueryType shouldBe null
          original.setQueryType(None).getQueryType shouldBe null
          original.setQueryType(Some(queryType)).getQueryType shouldBe queryType
        }

        it("facets") {

          val facets = Seq("f1")
          original.getFacets shouldBe null
          original.setFacets(None).getFacets shouldBe null
          original.setFacets(Some(facets)).getFacets should contain theSameElementsAs facets
        }

        it("search mode") {

          val searchMode = SearchMode.ALL
          original.getSearchMode shouldBe null
          original.setSearchMode(None).getSearchMode shouldBe null
          original.setSearchMode(Some(searchMode)).getSearchMode shouldBe searchMode
        }
      }
    }
  }
}
