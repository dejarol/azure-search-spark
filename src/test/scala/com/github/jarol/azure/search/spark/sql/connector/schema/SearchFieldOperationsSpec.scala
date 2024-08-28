package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, SearchFieldFactory, StructFieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructField}

class SearchFieldOperationsSpec
  extends BasicSpec
    with SearchFieldFactory
      with StructFieldFactory {

  describe(anInstanceOf[SearchFieldOperations]) {
    describe(SHOULD) {
      it(s"evaluate if the field has the same name with respect to a ${nameOf[StructField]}") {

        val name = "date"
        createSearchField(name, SearchFieldDataType.STRING)
          .sameNameOf(
            createStructField(name, DataTypes.IntegerType)
          ) shouldBe true
      }
    }
  }
}
