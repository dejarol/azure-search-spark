package io.github.dejarol.azure.search.spark.connector.write

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.core.schema.SearchFieldFeature
import io.github.dejarol.azure.search.spark.connector.write.config.SearchFieldAnalyzerType
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}

class SearchFieldActionsSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val feature = SearchFieldFeature.SEARCHABLE
  private lazy val analyzer = LexicalAnalyzerName.AR_LUCENE

  /**
   * Creates a sample Search field for testing
   * @return a sample Search field
   */

  private def getSampleField: SearchField = createSearchField("hello", SearchFieldDataType.STRING)

  /**
   * Assert that an action added an analyzer to a Search field definition
   * @param analyzerType analyzer type
   * @param analyzerName analyzer to add
   */

  private def assertAddedAnalyzer(
                                   input: SearchField,
                                   analyzerType: SearchFieldAnalyzerType,
                                   analyzerName: LexicalAnalyzerName
                                 ): Unit = {

    // Original field should have no analyzer,
    analyzerType.getFromField(input) shouldBe null

    // while the transformed (actual) field should have it
    val actual = SearchFieldActions.forSettingAnalyzer(analyzerType, analyzerName).apply(input)
    analyzerType.getFromField(actual) shouldBe analyzerName
  }

  describe(`object`[SearchFieldActions.type ]) {
    describe(SHOULD) {
      describe("define actions that") {
        it("enable a feature") {

          val sampleField = getSampleField
          sampleField should not be enabledFor(feature)
          val actual = SearchFieldActions.forEnablingFeature(feature).apply(sampleField)
          actual shouldBe enabledFor(feature)
        }

        it("disable a feature") {

          // First enable the feature
          val enabledField = feature.enableOnField(getSampleField)
          enabledField shouldBe enabledFor(feature)

          // Now, let the action disable it
          val actual = SearchFieldActions.forDisablingFeature(feature).apply(enabledField)
          actual should not be enabledFor(feature)
        }

        it("enable/disable a feature") {

          val (sampleField, feature) = (getSampleField, SearchFieldFeature.SEARCHABLE)
          sampleField should not be enabledFor(feature)

          // Assert that the action enables the feature
          val enabled = SearchFieldActions.forEnablingOrDisablingFeature(feature, flag = true).apply(sampleField)
          enabled shouldBe enabledFor(feature)

          // Assert that the action disables the feature
          val disabled = SearchFieldActions.forEnablingOrDisablingFeature(feature, flag = false).apply(enabled)
          disabled should not be enabledFor(feature)
        }

        it("add analyzers") {

          forAll(SearchFieldAnalyzerType.values().toSeq) {
            analyzerType =>
              assertAddedAnalyzer(
                getSampleField,
                analyzerType,
                analyzer
              )
          }
        }

        it("set the vector search profile") {

          val (sampleField, profile) = (getSampleField, "profileName")
          sampleField.getVectorSearchProfileName shouldBe null
          val actual = SearchFieldActions.forSettingVectorSearchProfile(profile).apply(sampleField)
          actual.getVectorSearchProfileName shouldBe profile
        }

        it("folds many actions at once") {

          val (sampleField, feature, profile) = (getSampleField, SearchFieldFeature.SEARCHABLE, "hello")
          val actions = Seq(
            SearchFieldActions.forEnablingFeature(feature),
            SearchFieldActions.forSettingVectorSearchProfile(profile)
          )

          feature.isEnabledOnField(sampleField) shouldBe false
          sampleField.getVectorSearchProfileName shouldBe null
          val actual = SearchFieldActions.forFoldingActions(
            actions
          ).apply(sampleField)

          feature.isEnabledOnField(actual) shouldBe true
          actual.getVectorSearchProfileName shouldBe profile
        }
      }
    }
  }
}
