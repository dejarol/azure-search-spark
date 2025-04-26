package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.core.DataTypeException
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}

class SearchFieldOperationsSpec
  extends BasicSpec
    with FieldFactory {

  /**
   * Creates an anonymous Search field
   * @param `type` Search field type
   * @return an anonymous Search field
   */

  private def createAnonymousField(`type`: SearchFieldDataType): SearchField = createSearchField("hello", `type`)

  describe(anInstanceOf[SearchFieldOperations]) {
    describe(SHOULD) {
      describe("evaluate if a Search field is") {
        it("a string") {

          createAnonymousField(SearchFieldDataType.STRING).isString shouldBe true
          createAnonymousField(SearchFieldDataType.INT64).isString shouldBe false
        }

        it("a number") {

          createAnonymousField(SearchFieldDataType.STRING).isNumeric shouldBe false
          createAnonymousField(SearchFieldDataType.INT32).isNumeric shouldBe true
          createAnonymousField(SearchFieldDataType.INT64).isNumeric shouldBe true
          createAnonymousField(SearchFieldDataType.DOUBLE).isNumeric shouldBe true
        }

        it("a boolean") {

          createAnonymousField(SearchFieldDataType.STRING).isBoolean shouldBe false
          createAnonymousField(SearchFieldDataType.BOOLEAN).isBoolean shouldBe true
        }

        it("a datetime") {

          createAnonymousField(SearchFieldDataType.DATE_TIME_OFFSET).isDateTime shouldBe true
          createAnonymousField(SearchFieldDataType.INT64).isDateTime shouldBe false
        }

        it("atomic") {

          createAnonymousField(SearchFieldDataType.STRING).isAtomic shouldBe true
          createAnonymousField(SearchFieldDataType.INT64).isAtomic shouldBe true
          createAnonymousField(SearchFieldDataType.BOOLEAN).isAtomic shouldBe true
          createAnonymousField(SearchFieldDataType.DATE_TIME_OFFSET).isAtomic shouldBe true
        }

        it("a collection") {

          createAnonymousField(
            createCollectionType(
              SearchFieldDataType.STRING
            )
          ).isCollection shouldBe true

          createAnonymousField(SearchFieldDataType.DATE_TIME_OFFSET).isCollection shouldBe false
        }

        it("a complex field") {

          createAnonymousField(SearchFieldDataType.COMPLEX).isComplex shouldBe true
          createAnonymousField(SearchFieldDataType.BOOLEAN).isComplex shouldBe false
        }

        describe("extract both safely and safely") {
          it("the collection inner type") {

            // For a collection field, we expect the safe operation to return a defined result,
            // and the unsafe operation to run successfully
            val innerType = SearchFieldDataType.STRING
            val collectionField = createAnonymousField(
              createCollectionType(
                innerType
              )
            )

            val maybeCollectionInnerType = collectionField.safeCollectionInnerType
            maybeCollectionInnerType shouldBe defined
            maybeCollectionInnerType.get shouldBe innerType

            // For a non-collection field, we expect the safe operation to return an empty result,
            // and the unsafe operation to fail
            val nonCollectionField = createAnonymousField(SearchFieldDataType.STRING)
            nonCollectionField.safeCollectionInnerType shouldBe empty
            a [DataTypeException] shouldBe thrownBy {
              nonCollectionField.unsafeCollectionInnerType
            }
          }

          it("the subfields") {

            // For a complex field, we expect the safe operation to return a defined result,
            // and the unsafe operation to run successfully
            val subFields = Seq(
              createSearchField("id", SearchFieldDataType.INT32),
              createSearchField("description", SearchFieldDataType.STRING)
            )

            val complexField = createComplexField("details", subFields)
            val maybeSubFields = complexField.safeSubFields
            maybeSubFields shouldBe defined
            maybeSubFields.get should contain theSameElementsAs subFields

            // For a non-complex field, we expect the safe operation to return an empty result,
            // and the unsafe operation to fail
            val nonComplexField = createAnonymousField(SearchFieldDataType.STRING)
            nonComplexField.safeSubFields shouldBe empty
            a [DataTypeException] shouldBe thrownBy {
              nonComplexField.unsafeSubFields
            }
          }
        }
      }

      it("a feature is enabled") {

        val feature = SearchFieldFeature.KEY
        val field = createAnonymousField(SearchFieldDataType.STRING)
        field should not be enabledFor(feature)
        val enabled = feature.enableOnField(field)
        enabled shouldBe enabledFor(feature)
      }

      it("apply some actions") {

        lazy val notSearchable: SearchFieldAction = (field: SearchField) => field.setSearchable(false)
        lazy val makeFacetable: SearchFieldAction = (field: SearchField) => field.setFacetable(true)

        val field = createAnonymousField(SearchFieldDataType.INT32)
        val transformedField = field.applyActions(notSearchable, makeFacetable)
        transformedField should not be enabledFor(SearchFieldFeature.SEARCHABLE)
        transformedField shouldBe enabledFor(SearchFieldFeature.FACETABLE)
      }
    }
  }
}
