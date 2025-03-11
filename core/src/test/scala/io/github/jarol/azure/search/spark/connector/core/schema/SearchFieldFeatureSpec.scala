package io.github.jarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.jarol.azure.search.spark.connector.core.{BasicSpec, FieldFactory}

class SearchFieldFeatureSpec
  extends BasicSpec
    with FieldFactory {

  describe(anInstanceOf[SearchFieldFeature]) {
    describe(SHOULD) {
      it(s"enable a feature on a ${nameOf[SearchField]}") {

        val field = createSearchField("first", SearchFieldDataType.STRING)
        forAll(SearchFieldFeature.values().toSeq) {
          feature =>
            field should not be enabledFor(feature)
            feature.enableOnField(field) shouldBe enabledFor(feature)
        }
      }

      it(s"disable a feature on a ${nameOf[SearchField]}") {

        val field = createSearchField("second", SearchFieldDataType.INT32)
        forAll(SearchFieldFeature.values().toSeq) {
          feature =>
            field should not be enabledFor(feature)
            val enabled = feature.enableOnField(field)
            enabled shouldBe enabledFor(feature)
            val disabled = feature.disableOnField(enabled)
            disabled should not be enabledFor(feature)
        }
      }
    }
  }
}
