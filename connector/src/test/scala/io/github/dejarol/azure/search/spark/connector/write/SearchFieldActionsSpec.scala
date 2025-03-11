package io.github.dejarol.azure.search.spark.connector.write

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}
import io.github.dejarol.azure.search.spark.connector.core.FieldFactory
import io.github.dejarol.azure.search.spark.connector.write.config.SearchFieldAnalyzerType

class SearchFieldActionsSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val sampleField = createSearchField("hello", SearchFieldDataType.STRING)
  private lazy val feature = SearchFieldFeature.SEARCHABLE
  private lazy val analyzer = LexicalAnalyzerName.AR_LUCENE

  /**
   * Assert that an action added an analyzer to a Search field definition
   * @param analyzerType analyzer type
   * @param analyzerName analyzer to add
   */

  private def assertAddedAnalyzer(
                                   analyzerType: SearchFieldAnalyzerType,
                                   analyzerName: LexicalAnalyzerName
                                 ): Unit = {

    // Original field should have no analyzer,
    analyzerType.getFromField(sampleField) shouldBe null

    // while the transformed (actual) field should have it
    val actual = SearchFieldActions.forSettingAnalyzer(analyzerType, analyzerName).apply(sampleField)
    analyzerType.getFromField(actual) shouldBe analyzerName
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

          forAll(SearchFieldAnalyzerType.values().toSeq) {
            analyzerType =>
              assertAddedAnalyzer(
                analyzerType,
                analyzer
              )
          }
        }
      }
    }
  }
}
