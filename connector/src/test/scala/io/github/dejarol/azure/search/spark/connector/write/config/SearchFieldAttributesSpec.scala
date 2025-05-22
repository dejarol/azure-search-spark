package io.github.dejarol.azure.search.spark.connector.write.config

import com.azure.search.documents.indexes.models.LexicalAnalyzerName
import io.github.dejarol.azure.search.spark.connector.core.schema.{SearchFieldAction, SearchFieldFeature}
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, JsonMixIns}

class SearchFieldAttributesSpec
  extends BasicSpec
    with JsonMixIns {

  private lazy val lexicalAnalyzer = LexicalAnalyzerName.AR_LUCENE
  private lazy val emptyOptions = SearchFieldAttributes(
    analyzer = None,
    facetable = None,
    filterable = None,
    indexAnalyzer = None,
    key = None,
    retrievable = None,
    searchAnalyzer = None,
    searchable = None,
    sortable = None,
    vectorSearchProfile = None
  )

  /**
   * Creates a copy of the options by defining a value for a single field,
   * retrieves the copy's action and checks that it includes the expected action's description
   * @param value value to include within empty options
   * @param copyFunction function that creates a copy of the options
   * @param expectedAction expected action
   * @tparam A value type
   */

  private def assertOptionsAction[A](
                                      value: A,
                                      copyFunction: (SearchFieldAttributes, A) => SearchFieldAttributes,
                                      expectedAction: SearchFieldAction
                                    ): Unit = {

    val copy = copyFunction(emptyOptions, value)
    val actualAction = copy.toAction
    actualAction shouldBe defined
    actualAction.get shouldBe SearchFieldActions.forFoldingActions(
      Seq(expectedAction)
    )
  }

  describe(anInstanceOf[SearchFieldAttributes]) {
    describe(SHOULD) {
      describe("return an overall action that") {
        describe("sets") {
          it("the analyzer") {

            assertOptionsAction[LexicalAnalyzerName](
              lexicalAnalyzer,
              (o, v) => o.copy(analyzer = Some(v)),
              SearchFieldActions.forSettingAnalyzer(lexicalAnalyzer)
            )
          }

          it("the index analyzer") {

            assertOptionsAction[LexicalAnalyzerName](
              lexicalAnalyzer,
              (o, v) => o.copy(indexAnalyzer = Some(v)),
              SearchFieldActions.forSettingIndexAnalyzer(lexicalAnalyzer)
            )
          }

          it("the search analyzer") {

            assertOptionsAction[LexicalAnalyzerName](
              lexicalAnalyzer,
              (o, v) => o.copy(searchAnalyzer = Some(v)),
              SearchFieldActions.forSettingSearchAnalyzer(lexicalAnalyzer)
            )
          }

          it("the vector search profile") {

            val profileName = "profile"
            assertOptionsAction[String](
              profileName,
              (o, v) => o.copy(vectorSearchProfile = Some(v)),
              SearchFieldActions.forSettingVectorSearchProfile(profileName)
            )
          }
        }

        describe("marks the field as") {
          it("facetable") {

            assertOptionsAction[Boolean](
              true,
              (o, v) => o.copy(facetable = Some(v)),
              SearchFieldActions.forEnablingFeature(SearchFieldFeature.FACETABLE)
            )
          }

          it("filterable") {

            assertOptionsAction[Boolean](
              true,
              (o, v) => o.copy(filterable = Some(v)),
              SearchFieldActions.forEnablingFeature(SearchFieldFeature.FILTERABLE)
            )
          }

          it("key") {

            assertOptionsAction[Boolean](
              true,
              (o, v) => o.copy(key = Some(v)),
              SearchFieldActions.forEnablingFeature(SearchFieldFeature.KEY)
            )
          }

          it("retrievable") {

            assertOptionsAction[Boolean](
              false,
              (o, v) => o.copy(retrievable = Some(v)),
              SearchFieldActions.forEnablingFeature(SearchFieldFeature.HIDDEN)
            )
          }

          it("sortable") {

            assertOptionsAction[Boolean](
              true,
              (o, v) => o.copy(sortable = Some(v)),
              SearchFieldActions.forEnablingFeature(SearchFieldFeature.SORTABLE)
            )
          }

          it("searchable") {

            assertOptionsAction[Boolean](
              true,
              (o, v) => o.copy(searchable = Some(v)),
              SearchFieldActions.forEnablingFeature(SearchFieldFeature.SEARCHABLE)
            )
          }
        }

        it("folds all defined actions") {

          val copy = emptyOptions.copy(
            analyzer = Some(lexicalAnalyzer),
            facetable = Some(false)
          )

          val expectedAction = SearchFieldActions.forFoldingActions(
            Seq(
              SearchFieldActions.forSettingAnalyzer(lexicalAnalyzer),
              SearchFieldActions.forDisablingFeature(SearchFieldFeature.FACETABLE)
            )
          )

          val actualAction = copy.toAction
          actualAction shouldBe defined
          actualAction.get shouldBe expectedAction
        }
      }

      describe("be deserialized from json when") {
        it("all attributes are defined") {

          val (analyzer, indexAnalyzer, searchAnalyzer, profile) = (
            LexicalAnalyzerName.AR_LUCENE,
            LexicalAnalyzerName.BN_MICROSOFT,
            LexicalAnalyzerName.WHITESPACE,
            "hello"
          )

          val json =
            s"""
              |{
              |  "analyzer": "$analyzer",
              |  "facetable": true,
              |  "filterable": false,
              |  "indexAnalyzer": "$indexAnalyzer",
              |  "key": true,
              |  "retrievable": false,
              |  "searchAnalyzer": "$searchAnalyzer",
              |  "searchable": false,
              |  "sortable": true,
              |  "vectorSearchProfile": "$profile"
              |}
              |""".stripMargin

          val actual = readValueAs[SearchFieldAttributes](json)
          actual.analyzer shouldBe Some(analyzer)
          actual.facetable shouldBe Some(true)
          actual.filterable shouldBe Some(false)
          actual.indexAnalyzer shouldBe Some(indexAnalyzer)
          actual.key shouldBe Some(true)
          actual.retrievable shouldBe Some(false)
          actual.searchAnalyzer shouldBe Some(searchAnalyzer)
          actual.searchable shouldBe Some(false)
          actual.sortable shouldBe Some(true)
          actual.vectorSearchProfile shouldBe Some(profile)
        }

        it("some attributes are missing") {

          val json =
            """
              |{
              |  "key": true
              |}
              |""".stripMargin

          val actual = readValueAs[SearchFieldAttributes](json)
          actual.analyzer shouldBe None
          actual.facetable shouldBe None
          actual.filterable shouldBe None
          actual.indexAnalyzer shouldBe None
          actual.key shouldBe Some(true)
          actual.retrievable shouldBe None
          actual.searchAnalyzer shouldBe None
          actual.searchable shouldBe None
          actual.sortable shouldBe None
          actual.vectorSearchProfile shouldBe None
        }

        it("some attributes have invalid values") {

          val json =
            s"""
               |{
               |  "analyzer": "hello"
               |}
               |""".stripMargin

          val actual = readValueAs[SearchFieldAttributes](json)
          actual.analyzer shouldBe empty
        }
      }
    }
  }
}
