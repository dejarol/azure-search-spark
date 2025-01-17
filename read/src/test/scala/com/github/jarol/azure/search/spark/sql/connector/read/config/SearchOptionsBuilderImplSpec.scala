package com.github.jarol.azure.search.spark.sql.connector.read.config

import com.azure.search.documents.models.{QueryType, SearchMode}
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.config.ConfigException
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class SearchOptionsBuilderImplSpec
  extends BasicSpec {

  /**
   * Create a configuration instance from a simple map
   * @param map input map
   * @return a configuration instance
   */

  private def createConfig(map: Map[String, String]): SearchOptionsBuilderImpl = {

    SearchOptionsBuilderImpl(
      CaseInsensitiveMap(map)
    )
  }

  private lazy val emptyConfig = createConfig(Map.empty)
  private lazy val (first, second) = ("first", "second")

  describe(anInstanceOf[SearchOptionsBuilderImpl]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the search text") {

          emptyConfig.searchText shouldBe empty
          val expected = "hello"
          createConfig(
            Map(
              SearchOptionsBuilderImpl.SEARCH -> expected
            )
          ).searchText shouldBe Some(expected)
        }

        it("the filter to apply on index documents") {

          val expected = "filterValue"
          emptyConfig.filter shouldBe empty
          createConfig(
            Map(
              SearchOptionsBuilderImpl.FILTER -> expected
            )
          ).filter shouldBe Some(expected)
        }

        it("the pushed predicate") {

          val expected = "hello"
          emptyConfig.pushedPredicate shouldBe empty
          createConfig(
            Map(
              SearchOptionsBuilderImpl.PUSHED_PREDICATE -> expected
            )
          ).pushedPredicate shouldBe Some(expected)
        }

        it("the combined filter") {

          emptyConfig.combinedFilter shouldBe empty
          val (first, second) = ("hello", "world")
          createConfig(
            Map(
              SearchOptionsBuilderImpl.FILTER -> first
            )
          ).combinedFilter shouldBe Some(first)

          createConfig(
            Map(
              SearchOptionsBuilderImpl.PUSHED_PREDICATE -> second
            )
          ).combinedFilter shouldBe Some(second)

          createConfig(
            Map(
              SearchOptionsBuilderImpl.FILTER -> first,
              SearchOptionsBuilderImpl.PUSHED_PREDICATE -> second
            )
          ).combinedFilter shouldBe Some(s"($first) and ($second)")
        }

        it("the search fields to select") {

          val expected = Seq("f1", "f2")
          emptyConfig.select shouldBe empty
          val actual: Option[Seq[String]] = createConfig(
            Map(
              SearchOptionsBuilderImpl.SELECT_CONFIG -> expected.mkString(",")
            )
          ).select

          actual shouldBe defined
          actual.get should contain theSameElementsAs expected
        }

        it("the query type") {

          emptyConfig.queryType shouldBe empty

          // Valid case
          val expected = QueryType.SEMANTIC
          createConfig(
            Map(
              SearchOptionsBuilderImpl.QUERY_TYPE -> expected.name()
            )
          ).queryType shouldBe Some(expected)

          // Invalid case
          a [ConfigException] shouldBe thrownBy {
            createConfig(
              Map(
                SearchOptionsBuilderImpl.QUERY_TYPE -> "hello"
              )
            ).queryType
          }
        }

        it("the search mode") {

          emptyConfig.searchMode shouldBe empty

          // Valid case
          val expected = SearchMode.ANY
          createConfig(
            Map(
              SearchOptionsBuilderImpl.SEARCH_MODE -> expected.name()
            )
          ).searchMode shouldBe Some(expected)

          // Invalid case
          a [ConfigException] shouldBe thrownBy {
            createConfig(
              Map(
                SearchOptionsBuilderImpl.SEARCH_MODE -> "hello"
              )
            ).searchMode
          }
        }

        it("facets") {

          emptyConfig.facets shouldBe empty
          val expected = Seq("hello", "world")
          val actual = createConfig(
            Map(
              SearchOptionsBuilderImpl.FACETS -> expected.mkString("|")
            )
          ).facets

          actual shouldBe defined
          actual.get should contain theSameElementsAs expected
        }
      }

      it("search fields") {

        emptyConfig.searchFields shouldBe empty
        val expected = Seq("hello", "world")
        val actual = createConfig(
          Map(
            SearchOptionsBuilderImpl.SEARCH_FIELDS -> expected.mkString(",")
          )
        ).searchFields

        actual shouldBe defined
        actual.get should contain theSameElementsAs expected
      }

      describe("let a user add") {
        it("a filter") {

          emptyConfig.addFilter(first).filter shouldBe Some(first)
          createConfig(
            Map(
              SearchOptionsBuilderImpl.FILTER -> first
            )
          ).addFilter(second).filter shouldBe Some(s"($first) and ($second)")
        }

        it("a facet") {

          val firstResult = emptyConfig.addFacet(first).facets
          firstResult shouldBe defined
          firstResult.get should contain theSameElementsAs Seq(first)

          val secondResult = createConfig(
            Map(
              SearchOptionsBuilderImpl.FACETS -> first
            )
          ).addFacet(second).facets

          secondResult shouldBe defined
          secondResult.get should contain theSameElementsAs Seq(first, second)
        }
      }
    }
  }
}
