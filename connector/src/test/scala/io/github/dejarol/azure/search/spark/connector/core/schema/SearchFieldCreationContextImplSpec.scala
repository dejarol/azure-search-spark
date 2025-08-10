package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}
import org.scalamock.scalatest.MockFactory

class SearchFieldCreationContextImplSpec
  extends BasicSpec
    with FieldFactory
      with MockFactory {

  /**
   * Create a mock [[SearchFieldAction]] that returns a copy of the field
   * (in terms on the field name and type) with the given feature enabled
   * @param field original field
   * @param feature feature to enable
   * @return a mock action
   */

  private def mockEnablingAction(
                                  field: SearchField,
                                  feature: SearchFieldFeature
                                ): SearchFieldAction = {

    // Create a mock action that returns a copy of the field
    // (in terms on the field name and type) with the feature enabled
    val action = mock[SearchFieldAction]
    (action.apply _).expects(field).returns(
      feature.enableOnField(
        createSearchField(
          field.getName,
          field.getType
        )
      )
    ).repeated(0 to 1)

    // Return the mock
    action
  }

  /**
   * Create a set of rules with no exclusion list and a single action
   * @param key single key for field actions
   * @param field input field (for mocking the action)
   * @param feature feature to enable (for mocking the action)
   * @return a set of rules
   */

  private def createContextWithSingleAction(
                                             key: String,
                                             field: SearchField,
                                             feature: SearchFieldFeature
                                           ): SearchFieldCreationContext = {

    SearchFieldCreationContextImpl(
      None,
      Map(
        key -> mockEnablingAction(
          field, feature
        )
      )
    )
  }

  /**
   * Create a set of rules with just one field excluded from geo conversion and no actions
   * @param name name of the field to exclude from geo conversion
   * @return a set of rules
   */

  private def createRulesWithExcludedGeoConversion(name: String): SearchFieldCreationContext = {

    SearchFieldCreationContextImpl(
      Some(
        Seq(name)
      ),
      Map.empty
    )
  }

  describe(anInstanceOf[SearchFieldCreationContextImpl]) {
    describe(SHOULD) {
      describe("alter a field when") {
        it("a matching field name is found") {

          val feature = SearchFieldFeature.SEARCHABLE
          val field = createSearchField("hello", SearchFieldDataType.STRING)
          val rules = createContextWithSingleAction(field.getName, field, feature)

          field should not be enabledFor(feature)
          val altered = rules.maybeApplyActions(field, field.getName)
          altered shouldBe enabledFor(feature)
        }
      }

      describe("mark a field as excluded from geo conversion when") {
        it("a matching field name is found") {

          val value = "hello"
          createRulesWithExcludedGeoConversion(value)
            .shouldBeExcludedFromGeoConversion(value) shouldBe true
        }
      }
    }

    describe(SHOULD_NOT) {
      describe("alter a field when") {
        it("no matching field name is found") {

          val feature = SearchFieldFeature.SEARCHABLE
          val field = createSearchField("world", SearchFieldDataType.STRING)
          val rules = createContextWithSingleAction("hello", field, feature)

          field should not be enabledFor(feature)
          val altered = rules.maybeApplyActions(field, field.getName)
          altered should not be enabledFor(feature)
        }
      }

      describe("mark a field as excluded from geo conversion when") {
        it("no matching field name is found") {

          val value = "hello"
          createRulesWithExcludedGeoConversion("world")
            .shouldBeExcludedFromGeoConversion(value) shouldBe false
        }
      }
    }
  }
}
