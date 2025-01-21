package io.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.jarol.azure.search.spark.sql.connector.core.config.ConfigException
import io.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}

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
            InferSchema.forIndex(
              "index",
              Seq(hiddenField)
            )
          }
        }
      }

      describe("infer schema") {
        it("only for non-hidden and selected fields") {

          // no selection provided
          val expectedSize = searchFields.count {
            sf => !sf.isHidden
          }

          InferSchema.forIndex("index", searchFields) should have size expectedSize
        }
      }
    }
  }
}
