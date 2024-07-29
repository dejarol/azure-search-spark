package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.BasicSpec

class InferSchemaSpec
  extends BasicSpec {

  private lazy val (f1, f2, f3) = ("stringField", "intField", "dateField")
  private lazy val searchFields: Seq[SearchField] = Seq(
    new SearchField("hidden", SearchFieldDataType.STRING).setHidden(true),
    new SearchField(f1, SearchFieldDataType.STRING).setHidden(false),
    new SearchField(f2, SearchFieldDataType.INT32).setHidden(false),
    new SearchField(f3, SearchFieldDataType.DATE_TIME_OFFSET).setHidden(false)
  )

  describe(`object`[InferSchema.type ]) {
    describe(SHOULD) {
      describe("filter search fields") {
        it("taking only visible (not hidden) fields") {

          InferSchema.filterSearchFields(
            searchFields,
            None
          ) should contain theSameElementsAs searchFields.filter {
            sf => !sf.isHidden
          }
        }

        it("included in a selection list") {

          val selectionList = Seq(f2, f3)
          InferSchema.filterSearchFields(
            searchFields,
            Some(selectionList)
          ) should contain theSameElementsAs searchFields.filter {
            sf => !sf.isHidden && selectionList.contains(sf.getName)
          }
        }
      }
    }
  }
}
