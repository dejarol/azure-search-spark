package io.github.dejarol.azure.search.spark.connector.write.config

import io.github.dejarol.azure.search.spark.connector.core.config.SearchConfig
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.DataTypes

class SearchFieldCreationOptionsSpec
  extends BasicSpec
    with FieldFactory {

  /**
   * Creates an instance
   *
   * @param options      options
   * @param actionColumn action column
   * @return an option instance for testing
   */

  private def createOptions(
                             options: Option[Map[String, String]],
                             actionColumn: Option[String]
                           ): SearchFieldCreationOptions = {

    SearchFieldCreationOptions(
      new SearchConfig(
        CaseInsensitiveMap[String](
          options.getOrElse(Map.empty)
        )
      ),
      actionColumn
    )
  }

  describe(`object`[SearchFieldCreationOptions]) {
    describe(SHOULD) {
      describe("enrich an existing set of attributes when") {
        it("no key field has been defined") {

          val enriched = SearchFieldCreationOptions.enrichAttributes(
            Map.empty[String, SearchFieldAttributes]
          )

          enriched should have size 1
          enriched should contain key SearchFieldCreationOptions.DEFAULT_ID_COLUMN
          enriched(SearchFieldCreationOptions.DEFAULT_ID_COLUMN) shouldBe
            SearchFieldAttributes.empty().withKeyFieldEnabled
        }

        it(s"an ${SearchFieldCreationOptions.DEFAULT_ID_COLUMN} field is defined, but not key-enabled") {

          val attributeForId = SearchFieldAttributes.empty()
          val original = Map(
            SearchFieldCreationOptions.DEFAULT_ID_COLUMN -> attributeForId
          )

          val enriched = SearchFieldCreationOptions.enrichAttributes(original)
          enriched should have size 1
          enriched should contain key SearchFieldCreationOptions.DEFAULT_ID_COLUMN
          val enrichedAttributes = enriched(SearchFieldCreationOptions.DEFAULT_ID_COLUMN)
          enrichedAttributes shouldBe attributeForId.withKeyFieldEnabled
        }
      }
    }

    describe("return an-existing set of attributes as-is when") {
      it("a key field has been defined") {

        val attributeForId = SearchFieldAttributes.empty().withKeyFieldEnabled
        val original = Map("key" -> attributeForId)

        val enriched = SearchFieldCreationOptions.enrichAttributes(original)
        enriched should have size 1
        enriched should contain key "key"
        enriched("key") shouldBe attributeForId
      }
    }
  }

  describe(anInstanceOf[SearchFieldCreationOptions]) {
    describe(SHOULD) {
      it("exclude index action column from a schema") {

        val actionCol = "action"
        val schema = Seq(
          createStructField(actionCol, DataTypes.StringType)
        )

        // Assert that the action column is excluded
        val optionsWithColumn = createOptions(None, Some(actionCol))
        optionsWithColumn.excludeIndexActionColumn(schema) shouldBe empty

        // Assert that the action column is not excluded
        val optionsWithoutColumn = createOptions(None, None)
        optionsWithoutColumn.excludeIndexActionColumn(schema) should contain theSameElementsAs schema
      }

      describe("collect") {
        describe("original attributes") {
          it("considering JSON-valid options") {

            // No options, no attributes
            createOptions(None, None).originalAttributes shouldBe empty

            // No valid options, no attributes
            createOptions(
              Some(
                Map(
                  "k1" -> "hello"
                )
              ), None
            ).originalAttributes shouldBe empty

            // One valid option, one attribute
            val v1 =
              s"""
                 |{
                 |  "key": true
                 |}
                 |""".stripMargin

            val originalAttributes = createOptions(
              Some(
                Map(
                  "k1" -> v1
                )
              ), None
            ).originalAttributes

            originalAttributes should have size 1
            originalAttributes should contain key "k1"
            originalAttributes("k1") shouldBe SearchFieldAttributes.empty().withKeyFieldEnabled
          }
        }

        describe("enriched attributes") {
          describe("adding a new key-value pair when") {
            it("no key field has been defined") {

              // No field is defined
              val emptyOptions = createOptions(None, None)
              val attributes = emptyOptions.enrichedAttributes
              attributes should have size 1
              attributes should contain key SearchFieldCreationOptions.DEFAULT_ID_COLUMN
              attributes(SearchFieldCreationOptions.DEFAULT_ID_COLUMN) shouldBe
                SearchFieldAttributes.empty().withKeyFieldEnabled

              // One field is defined, but not key-enabled
              val nonEmptyOptions = createOptions(
                Some(
                  Map(
                    "k1" -> "{}"
                  )
                ), None
              )

              val enrichedAttributes = nonEmptyOptions.enrichedAttributes
              enrichedAttributes should have size 2
              enrichedAttributes should contain key SearchFieldCreationOptions.DEFAULT_ID_COLUMN
              enrichedAttributes(SearchFieldCreationOptions.DEFAULT_ID_COLUMN) shouldBe
                SearchFieldAttributes.empty().withKeyFieldEnabled

              enrichedAttributes should contain key "k1"
              enrichedAttributes("k1") shouldBe SearchFieldAttributes.empty()
            }
          }

          it(s"enabling key attribute on ${SearchFieldCreationOptions.DEFAULT_ID_COLUMN} field") {

            val nonEmptyOptions = createOptions(
              Some(
                Map(
                  SearchFieldCreationOptions.DEFAULT_ID_COLUMN -> "{}"
                )
              ), None
            )

            // Original attributes and enriched attributes should be the same number,
            // but the enriched attributes should have the key field enabled
            val original = nonEmptyOptions.originalAttributes
            original should have size 1
            original should contain key SearchFieldCreationOptions.DEFAULT_ID_COLUMN
            original(SearchFieldCreationOptions.DEFAULT_ID_COLUMN) shouldBe
              SearchFieldAttributes.empty()

            val enrichedAttributes = nonEmptyOptions.enrichedAttributes
            enrichedAttributes should have size 1
            enrichedAttributes should contain key SearchFieldCreationOptions.DEFAULT_ID_COLUMN
            enrichedAttributes(SearchFieldCreationOptions.DEFAULT_ID_COLUMN) shouldBe
              SearchFieldAttributes.empty().withKeyFieldEnabled
          }
        }

        describe("actions only for") {
          it("JSON-valid attributes that define an action") {

            val v3 =
              s"""
                 |{
                 |  "filterable": true
                 |}
                 |""".stripMargin

            val map = Map(
              "k1" -> "hello",
              "k2" -> "{}",
              "k3" -> v3
            )

            val fieldActions = createOptions(Some(map), None).fieldActions
            fieldActions should have size 2
            fieldActions.keys should contain theSameElementsAs Seq("k3", SearchFieldCreationOptions.DEFAULT_ID_COLUMN)
          }
        }
      }

      describe("convert a Spark schema to a collection of Search fields") {
        it("excluding the index action column") {

          val (actionCol, second) = ("first", "second")
          val options = createOptions(None, Some(actionCol))
          val searchFields = options.toSearchFields(
            Seq(
              createStructField(actionCol, DataTypes.StringType),
              createStructField(second, DataTypes.IntegerType)
            )
          )

          searchFields should have size 1
          searchFields.head.getName shouldBe second
        }

        it("applying defined enrichment options") {

          val (keyCol, second) = ("key", "second")
          val keyColValue =
            s"""
               |{
               |  "key": true
               |}
               |""".stripMargin

          val options = createOptions(
            Some(
              Map(
                keyCol -> keyColValue
              )
            ),
            None
          )

          val searchFields = options.toSearchFields(
            Seq(
              createStructField(keyCol, DataTypes.StringType),
              createStructField(second, DataTypes.IntegerType)
            )
          ).map {
            sf => (sf.getName, sf)
          }.toMap

          searchFields should have size 2
          val maybeKeyCol = searchFields.get(keyCol)
          maybeKeyCol shouldBe defined
          maybeKeyCol.get.isKey shouldBe true

          val maybeSecond = searchFields.get(second)
          maybeSecond shouldBe defined
          maybeSecond.get.isKey shouldBe null
        }
      }
    }
  }
}
