package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructField}

class SearchFieldOperationsSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val first = "first"

  describe(anInstanceOf[SearchFieldOperations]) {
    describe(SHOULD) {
      it(s"evaluate if the field has the same name with respect to a ${nameOf[StructField]}") {

        val searchField = createSearchField(first, SearchFieldDataType.STRING)
        searchField
          .sameNameOf(
            createStructField(first, DataTypes.IntegerType)
          ) shouldBe true
      }

      it("enable some field features") {

        val searchField = createSearchField(first, SearchFieldDataType.STRING)
        val features = Seq(
          SearchFieldFeature.KEY,
          SearchFieldFeature.FILTERABLE
        )

        forAll(features) {
          _.isEnabledOnField(searchField) shouldBe false
        }

        val enabledField = searchField.enableFeatures(features: _*)
        forAll(features) {
          _.isEnabledOnField(enabledField) shouldBe true
        }
      }

      it("evaluate if a feature is enabled") {

        val searchField = createSearchField(first, SearchFieldDataType.STRING)
        val feature = SearchFieldFeature.KEY
        searchField.isEnabledFor(feature) shouldBe false
        val enabled = searchField.enableFeatures(feature)
        enabled.isEnabledFor(feature) shouldBe true
      }
    }
  }
}
