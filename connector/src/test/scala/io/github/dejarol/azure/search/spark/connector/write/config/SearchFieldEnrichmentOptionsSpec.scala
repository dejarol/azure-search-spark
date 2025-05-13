package io.github.dejarol.azure.search.spark.connector.write.config

import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.DataTypes

class SearchFieldEnrichmentOptionsSpec
  extends BasicSpec
    with FieldFactory {

  /**
   * Creates an instance
   * @param options options
   * @param actionColumn action column
   * @return an option instance for testing
   */

  private def createOptions(
                             options: Option[Map[String, String]],
                             actionColumn: Option[String]
                           ): SearchFieldEnrichmentOptions = {

    SearchFieldEnrichmentOptions(
      CaseInsensitiveMap[String](options.getOrElse(Map.empty)),
      actionColumn
    )
  }

  describe(anInstanceOf[SearchFieldEnrichmentOptions]) {
    describe(SHOULD) {
      it("exclude index action column from a schema") {

        val actionCol = "action"
        val schema = Seq(
          createStructField(actionCol, DataTypes.StringType)
        )

        // Assert that the action column is excluded
        val optionsWithColumn = createOptions(None, Some(actionCol))
        optionsWithColumn.excludeIndexActionColumn(schema) shouldBe empty

        // Assert that the action column is not excluded
        val optionsWithoutColumn = createOptions(None, None)
        optionsWithoutColumn.excludeIndexActionColumn(schema) should contain theSameElementsAs schema
      }

      describe("collect actions only for") {
        it("valid field options") {

          // TODO: test
        }
      }
    }
  }
}
