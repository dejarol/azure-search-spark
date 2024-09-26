package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}

class SearchFieldFeatureSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val field = createSearchField("first", SearchFieldDataType.STRING)

  /**
   * Assert that a feature has been implemented successfully
   * @param field test field
   * @param feature feature to test
   */

  private def assertCorrectFeature(
                                    field: SearchField,
                                    feature: SearchFieldFeature
                                  ): Unit = {

    // Before enabling, the feature should be disabled
    // After enabling, it should be true
    feature.isEnabledOnField(field) shouldBe false
    feature.isEnabledOnField(
      feature.enable(field)
    ) shouldBe true
  }

  describe(anInstanceOf[SearchFieldFeature]) {
    describe(SHOULD) {
      it(s"enable a feature on a ${nameOf[SearchField]}") {

        assertCorrectFeature(field, SearchFieldFeature.FACETABLE)
        assertCorrectFeature(field, SearchFieldFeature.FILTERABLE)
        assertCorrectFeature(field, SearchFieldFeature.HIDDEN)
        assertCorrectFeature(field, SearchFieldFeature.KEY)
        assertCorrectFeature(field, SearchFieldFeature.SEARCHABLE)
        assertCorrectFeature(field, SearchFieldFeature.SORTABLE)
      }
    }
  }
}
