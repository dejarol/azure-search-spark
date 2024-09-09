package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.config.ConfigException
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, FieldFactory}

class InferSchemaSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val (f1, f2, f3) = ("stringField", "intField", "dateField")
  private lazy val hiddenField = createSearchField("hidden", SearchFieldDataType.STRING).setHidden(true)
  private lazy val stringField = createSearchField(f1, SearchFieldDataType.STRING).setHidden(false)
  private lazy val intField = createSearchField(f2, SearchFieldDataType.INT32).setHidden(false)
  private lazy val dateField = createSearchField(f3, SearchFieldDataType.DATE_TIME_OFFSET).setHidden(false)

  private lazy val searchFields: Seq[SearchField] = Seq(
    hiddenField,
    stringField,
    intField,
    dateField
  )

  describe(`object`[InferSchema.type ]) {
    describe(SHOULD) {
      describe(s"throw an ${nameOf[InferSchemaException]}") {
        it("when all index fields are hidden") {

          an[InferSchemaException] shouldBe thrownBy {
            InferSchema.inferSchemaForIndex(
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

        it(s"throwing a ${nameOf[ConfigException]} if selected fields do not exist") {

          a[ConfigException] shouldBe thrownBy {

            InferSchema.selectFields(
              Seq(stringField, intField),
              Some(
                Seq(
                  dateField.getName
                )
              )
            )
          }
        }
      }

      describe("infer schema") {
        it("only for non-hidden and selected fields") {

          // no selection provided
          InferSchema.inferSchemaForIndex(
            "index",
            searchFields,
            None
          ) should have size searchFields.count {
            sf => !sf.isHidden
          }

          // selection provided
          val selection = Seq(
            stringField.getName,
            dateField.getName
          )

          InferSchema.inferSchemaForIndex(
            "index",
            searchFields,
            Some(selection)
          ) should have size searchFields.count {
            sf => !sf.isHidden && selection.contains(sf.getName)
          }
        }
      }
    }
  }
}
