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
        it("enabled for a feature") {

          val feature = SearchFieldFeature.KEY
          val field = createAnonymousField(SearchFieldDataType.STRING)
          field should not be enabledFor(feature)
          val enabled = feature.enableOnField(field)
          enabled shouldBe enabledFor(feature)
        }
      }

      describe("extract both safely and safely") {
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
          val nonComplexField = createAnonymousField(SearchFieldDataType.INT32)
          nonComplexField.safeSubFields shouldBe empty
          a [DataTypeException] shouldBe thrownBy {
            nonComplexField.unsafeSubFields
          }
        }
      }
    }
  }
}
