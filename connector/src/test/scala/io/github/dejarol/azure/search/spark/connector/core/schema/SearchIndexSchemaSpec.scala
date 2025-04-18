package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}

class SearchIndexSchemaSpec
  extends BasicSpec
    with FieldFactory {

  describe(anInstanceOf[SearchIndexSchema]) {
    describe(SHOULD) {
      it("retrieve Search fields by name, case-insensitively") {

        val fieldName = "InsertTime"
        val schema = SearchIndexSchema(
          Seq(
            createSearchField(fieldName, SearchFieldDataType.DATE_TIME_OFFSET)
          )
        )

        schema.get(fieldName) shouldBe defined
        schema.get(fieldName.toUpperCase) shouldBe defined
        schema.get(fieldName.toLowerCase) shouldBe defined
      }
    }
  }
}
