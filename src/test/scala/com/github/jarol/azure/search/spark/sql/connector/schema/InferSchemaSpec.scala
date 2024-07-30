package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.config.ConfigException

class InferSchemaSpec
  extends BasicSpec {

  private lazy val (f1, f2, f3) = ("stringField", "intField", "dateField")
  private lazy val hiddenField: SearchField = new SearchField("hidden", SearchFieldDataType.STRING).setHidden(true)
  private lazy val searchFields: Seq[SearchField] = Seq(
    hiddenField,
    new SearchField(f1, SearchFieldDataType.STRING).setHidden(false),
    new SearchField(f2, SearchFieldDataType.INT32).setHidden(false),
    new SearchField(f3, SearchFieldDataType.DATE_TIME_OFFSET).setHidden(false)
  )

  describe(`object`[InferSchema.type ]) {
    describe(SHOULD) {
      describe(s"throw an ${nameOf[InferSchemaException]}") {
        it("when all index fields are hidden") {

          an[InferSchemaException] shouldBe thrownBy {
            InferSchema.inferSchemaForExistingIndex(
              "index",
              Seq(hiddenField),
              None
            )
          }
        }
      }
      describe("select search fields") {
        it("included in a selection list") {

          val selectionList = Seq(f2, f3)
          InferSchema.selectFields(
            searchFields,
            Some(selectionList)
          ) should contain theSameElementsAs searchFields.filter {
            sf => !sf.isHidden && selectionList.contains(sf.getName)
          }
        }

        it(s"throwing a ${nameOf[ConfigException]} if selection fields do not exist") {

          // TODO
        }
      }

      describe("infer schema") {
        it("only for non-hidden and selected fields") {

          // TODO
        }
      }
    }
  }
}
