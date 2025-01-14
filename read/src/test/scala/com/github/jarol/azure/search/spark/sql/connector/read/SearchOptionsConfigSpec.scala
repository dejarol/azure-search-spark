package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.models.QueryType
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.config.ConfigException
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class SearchOptionsConfigSpec
  extends BasicSpec {

  /**
   * Create a configuration instance from a simple map
   * @param map input map
   * @return a configuration instance
   */

  private def createConfig(map: Map[String, String]): SearchOptionsConfig = {

    SearchOptionsConfig(
      CaseInsensitiveMap(map)
    )
  }

  private lazy val emptyConfig = createConfig(Map.empty)

  describe(anInstanceOf[SearchOptionsConfig]) {
    describe(SHOULD) {
      describe("retrieve") {

        it("the filter to apply on index documents") {

          val expected = "filterValue"
          emptyConfig.filter shouldBe empty
          createConfig(
            Map(
              SearchOptionsConfig.FILTER_CONFIG -> expected
            )
          ).filter shouldBe Some(expected)
        }

        it("the search fields to select") {

          val expected = Seq("f1", "f2")
          emptyConfig.select shouldBe empty
          val actual: Option[Seq[String]] = createConfig(
            Map(
              SearchOptionsConfig.SELECT_CONFIG -> expected.mkString(",")
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
              SearchOptionsConfig.QUERY_TYPE -> expected.name()
            )
          ).queryType shouldBe Some(expected)

          a [ConfigException] shouldBe thrownBy {

            createConfig(
              Map(
                SearchOptionsConfig.QUERY_TYPE -> "hello"
              )
            ).queryType
          }
        }

        it("facets") {

          emptyConfig.facets shouldBe empty
          val expected = Seq("hello", "world")
          val actual = createConfig(
            Map(
              SearchOptionsConfig.FACETS -> expected.mkString("|")
            )
          ).facets

          actual shouldBe defined
          actual.get should contain theSameElementsAs expected
        }
      }
    }
  }
}
