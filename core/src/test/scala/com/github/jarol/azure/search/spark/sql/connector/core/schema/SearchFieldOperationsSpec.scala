package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructField}

class SearchFieldOperationsSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val first = "first"
  private lazy val searchField = createSearchField(first, SearchFieldDataType.STRING)

  describe(anInstanceOf[SearchFieldOperations]) {
    describe(SHOULD) {
      describe("evaluate if") {
        it(s"the field has the same name with respect to a ${nameOf[StructField]}") {

          searchField.sameNameOf(
            createStructField(first, DataTypes.IntegerType)
          ) shouldBe true
        }

        it("a feature is enabled") {

          val feature = SearchFieldFeature.KEY
          searchField should not be enabledFor(feature)
          val enabled = feature.enableOnField(searchField)
          enabled shouldBe enabledFor(feature)
        }
      }

      it("apply some actions") {

        val transformedField = searchField.applyActions(
          SearchFieldActions.forDisablingFeature(SearchFieldFeature.SEARCHABLE),
          SearchFieldActions.forEnablingFeature(SearchFieldFeature.FACETABLE)
        )

        transformedField should not be enabledFor(SearchFieldFeature.SEARCHABLE)
        transformedField shouldBe enabledFor(SearchFieldFeature.FACETABLE)
      }
    }
  }
}
