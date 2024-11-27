package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}

class SearchFieldActionsSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val sampleField = createSearchField("hello", SearchFieldDataType.STRING)
  private lazy val feature = SearchFieldFeature.SEARCHABLE
  private lazy val analyzer = LexicalAnalyzerName.AR_LUCENE

  /**
   * Assert that an action added an analyzer to a Search field definition
   * @param analyzerName analyzer to add
   * @param actionSupplier function for generating the action responsible for adding the analyzer
   * @param analyzerGetter function for retrieving the analyzer from a Search field
   */

  private def assertAddedAnalyzer(
                                   analyzerName: LexicalAnalyzerName,
                                   actionSupplier: LexicalAnalyzerName => SearchFieldAction,
                                   analyzerGetter: SearchField => LexicalAnalyzerName
                                 ): Unit = {

    // Original field should have no analyzer, while the transformed (actual) field should have it
    analyzerGetter(sampleField) shouldBe null
    val actual = actionSupplier(analyzerName).apply(sampleField)
    analyzerGetter(actual) shouldBe analyzerName
  }

  describe(`object`[SearchFieldActions.type ]) {
    describe(SHOULD) {
      describe("define actions that") {
        it("enable a feature") {

          sampleField should not be enabledFor(feature)
          val actual = SearchFieldActions.forEnablingFeature(feature).apply(sampleField)
          actual shouldBe enabledFor(feature)
        }

        it("disable a feature") {

          // First enable the feature
          val enabledField = feature.enableOnField(sampleField)
          enabledField shouldBe enabledFor(feature)

          // Now, let the action disable it
          val actual = SearchFieldActions.forDisablingFeature(feature).apply(enabledField)
          actual should not be enabledFor(feature)
        }

        it("add analyzers") {

          assertAddedAnalyzer(analyzer, SearchFieldActions.forSettingAnalyzer, _.getAnalyzerName)
          assertAddedAnalyzer(analyzer, SearchFieldActions.forSettingSearchAnalyzer, _.getSearchAnalyzerName)
          assertAddedAnalyzer(analyzer, SearchFieldActions.forSettingIndexAnalyzer, _.getIndexAnalyzerName)
        }
      }
    }
  }
}
