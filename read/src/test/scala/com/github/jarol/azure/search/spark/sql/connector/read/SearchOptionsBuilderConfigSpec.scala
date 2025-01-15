package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.models.QueryType
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.config.ConfigException
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class SearchOptionsBuilderConfigSpec
  extends BasicSpec {

  /**
   * Create a configuration instance from a simple map
   * @param map input map
   * @return a configuration instance
   */

  private def createConfig(map: Map[String, String]): SearchOptionsBuilderConfig = {

    SearchOptionsBuilderConfig(
      CaseInsensitiveMap(map)
    )
  }

  private lazy val emptyConfig = createConfig(Map.empty)
  private lazy val (first, second) = ("first", "second")

  describe(anInstanceOf[SearchOptionsBuilderConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the search text") {

          emptyConfig.searchText shouldBe empty
          val expected = "hello"
          createConfig(
            Map(
              SearchOptionsBuilderConfig.SEARCH -> expected
            )
          ).searchText shouldBe Some(expected)
        }

        it("the filter to apply on index documents") {

          val expected = "filterValue"
          emptyConfig.filter shouldBe empty
          createConfig(
            Map(
              SearchOptionsBuilderConfig.FILTER -> expected
            )
          ).filter shouldBe Some(expected)
        }

        it("the search fields to select") {

          val expected = Seq("f1", "f2")
          emptyConfig.select shouldBe empty
          val actual: Option[Seq[String]] = createConfig(
            Map(
              SearchOptionsBuilderConfig.SELECT_CONFIG -> expected.mkString(",")
            )
          ).select

          actual shouldBe defined
          actual.get should contain theSameElementsAs expected
        }

        it("the query type") {

          emptyConfig.queryType shouldBe empty

          val expected = QueryType.SEMANTIC
          createConfig(
            Map(
              SearchOptionsBuilderConfig.QUERY_TYPE -> expected.name()
            )
          ).queryType shouldBe Some(expected)

          a [ConfigException] shouldBe thrownBy {

            createConfig(
              Map(
                SearchOptionsBuilderConfig.QUERY_TYPE -> "hello"
              )
            ).queryType
          }
        }

        it("facets") {

          emptyConfig.facets shouldBe empty
          val expected = Seq("hello", "world")
          val actual = createConfig(
            Map(
              SearchOptionsBuilderConfig.FACETS -> expected.mkString("|")
            )
          ).facets

          actual shouldBe defined
          actual.get should contain theSameElementsAs expected
        }
      }

      describe("let a user add") {
        it("a filter") {

          emptyConfig.withFilter(first).filter shouldBe Some(first)
          createConfig(
            Map(
              SearchOptionsBuilderConfig.FILTER -> first
            )
          ).withFilter(second).filter shouldBe Some(s"($first) and ($second)")
        }

        it("a facet") {

          val firstResult = emptyConfig.withFacet(first).facets
          firstResult shouldBe defined
          firstResult.get should contain theSameElementsAs Seq(first)

          val secondResult = createConfig(
            Map(
              SearchOptionsBuilderConfig.FACETS -> first
            )
          ).withFacet(second).facets

          secondResult shouldBe defined
          secondResult.get should contain theSameElementsAs Seq(first, second)
        }
      }
    }
  }
}
