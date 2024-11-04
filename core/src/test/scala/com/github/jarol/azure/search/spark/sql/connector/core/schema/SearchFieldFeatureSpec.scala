package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.scalatest.Inspectors

class SearchFieldFeatureSpec
  extends BasicSpec
    with FieldFactory
      with Inspectors {

  describe(anInstanceOf[SearchFieldFeature]) {
    describe(SHOULD) {
      it(s"enable a feature on a ${nameOf[SearchField]}") {

        val field = createSearchField("first", SearchFieldDataType.STRING)
        forAll(SearchFieldFeature.values().toSeq) {
          feature =>
            feature.isEnabledOnField(field) shouldBe false
            feature.isEnabledOnField(
              feature.enableOnField(field)
            ) shouldBe true
        }
      }

      it(s"disable a feature on a ${nameOf[SearchField]}") {

        val field = createSearchField("second", SearchFieldDataType.INT32)
        forAll(SearchFieldFeature.values().toSeq) {
          feature =>
            feature.isEnabledOnField(field) shouldBe false
            val enabled = feature.enableOnField(field)
            feature.isEnabledOnField(enabled) shouldBe true
            val disabled = feature.disableOnField(enabled)
            feature.isDisabledOnField(disabled) shouldBe true
        }
      }
    }
  }
}
