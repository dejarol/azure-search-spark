package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.config.ConfigException
import org.scalatest.Inspectors

class WriteConfigSpec
  extends BasicSpec
    with Inspectors {

  private def assertDefinedAndContaining(actual: Option[Seq[String]], expected: Seq[String]): Unit = {

    actual shouldBe defined
    actual.get should contain theSameElementsAs expected
  }

  private lazy val emptyConfig: WriteConfig = WriteConfig(Map.empty[String, String])

  describe(anInstanceOf[WriteConfig]) {
    describe(SHOULD) {
      describe("retrieve") {
        it("the batch size") {

          val batchSize = 25
          emptyConfig.batchSize shouldBe WriteConfig.DEFAULT_BATCH_SIZE_VALUE
          WriteConfig(
            Map(
              WriteConfig.BATCH_SIZE_CONFIG -> s"$batchSize"
            )
          ).batchSize shouldBe batchSize
        }

        it("the index action type") {

          emptyConfig.maybeUserSpecifiedAction shouldBe empty
          emptyConfig.overallAction shouldBe WriteConfig.DEFAULT_ACTION_TYPE
          val action = IndexActionType.UPLOAD
          val configMaps: Seq[Map[String, String]] = Seq(
            action.name(),
            action.toString,
            action.name().toLowerCase,
            action.toString.toUpperCase,
            action.toString.toLowerCase,
          ).map {
            value => Map(
              WriteConfig.ACTION_CONFIG -> value
            )
          }

          forAll(configMaps) {
            configMap =>

              val wConfig = WriteConfig(configMap)
              wConfig.maybeUserSpecifiedAction shouldBe Some(action)
              wConfig.overallAction shouldBe action
          }
        }

        it("the name of the index action type column") {

          val colName = "actionCol"
          emptyConfig.actionColumn shouldBe empty
          WriteConfig(
            Map(
              WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> colName
            )
          ).actionColumn shouldBe Some(colName)
        }

        describe("retrieve search field options") {
          it("throwing an exception for missing key fields") {

            a[ConfigException] shouldBe thrownBy {
              emptyConfig.searchFieldOptions
            }
          }

          it("collecting informations about features to enable") {

            val keyField = "hello"
            val (facetable, filterable) = (Seq("f1"), Seq("f2"))
            val (hidden, searchable, sortable) = (Seq("f3"), Seq("f4"), Seq("f5"))
            val indexActionColumn = "world"
            val options = WriteConfig(
              Map(
                s"${WriteConfig.CREATE_INDEX_PREFIX}${WriteConfig.KEY_FIELD_CONFIG}" -> keyField,
                s"${WriteConfig.CREATE_INDEX_PREFIX}${WriteConfig.DISABLE_FACETING_CONFIG}" -> facetable.mkString(","),
                s"${WriteConfig.CREATE_INDEX_PREFIX}${WriteConfig.DISABLE_FILTERING_CONFIG}" -> filterable.mkString(","),
                s"${WriteConfig.CREATE_INDEX_PREFIX}${WriteConfig.HIDDEN_FIELDS_CONFIG}" -> hidden.mkString(","),
                s"${WriteConfig.CREATE_INDEX_PREFIX}${WriteConfig.DISABLE_SEARCH_CONFIG}" -> searchable.mkString(","),
                s"${WriteConfig.CREATE_INDEX_PREFIX}${WriteConfig.DISABLE_SORTING_CONFIG}" -> sortable.mkString(","),
                WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> indexActionColumn
              )
            ).searchFieldOptions

            options.keyField shouldBe keyField
            assertDefinedAndContaining(options.disabledFromFaceting, facetable)
            assertDefinedAndContaining(options.disabledFromFiltering, filterable)
            assertDefinedAndContaining(options.hiddenFields, hidden)
            assertDefinedAndContaining(options.disabledFromSearch, searchable)
            assertDefinedAndContaining(options.disabledFromSorting, sortable)
            options.indexActionColumn shouldBe Some(indexActionColumn)
          }
        }
      }
    }
  }
}
