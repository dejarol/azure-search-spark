package io.github.jarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.jarol.azure.search.spark.connector.core.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructField}

class SearchFieldOperationsSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val first = "first"
  private lazy val searchField = createSearchField(first, SearchFieldDataType.STRING)
  private lazy val notSearchable: SearchFieldAction = (field: SearchField) => field.setSearchable(false)
  private lazy val makeFacetable: SearchFieldAction = (field: SearchField) => field.setFacetable(true)

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

        val transformedField = searchField.applyActions(notSearchable, makeFacetable)
        transformedField should not be enabledFor(SearchFieldFeature.SEARCHABLE)
        transformedField shouldBe enabledFor(SearchFieldFeature.FACETABLE)
      }
    }
  }
}
