package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}

class SearchFieldActionsSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val sampleField = createSearchField("hello", SearchFieldDataType.STRING)
  private lazy val feature = SearchFieldFeature.SEARCHABLE

  describe(`object`[SearchFieldActions.type ]) {
    describe(SHOULD) {
      describe("provide methods for creating actions that") {
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
      }
    }
  }
}
