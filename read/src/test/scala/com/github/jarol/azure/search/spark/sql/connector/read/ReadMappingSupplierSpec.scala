package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.MappingSupplierSpec
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.ReadConverter
import org.apache.spark.sql.types.DataTypes

class ReadMappingSupplierSpec
  extends MappingSupplierSpec[ReadConverter](ReadMappingSupplier) {

  describe(`object`[ReadMappingSupplier.type ]) {
    describe(SHOULD) {
      describe("return a Left for") {
        it("missing schema fields") {

          assertIsLeft(
            Seq(
              createStructField(first, DataTypes.StringType)
            ),
            Seq(
              createSearchField(second, SearchFieldDataType.STRING)
            )
          )
        }

        it("incompatible schema fields") {

          assertIsLeft(
            Seq(
              createStructField(first, DataTypes.StringType)
            ),
            Seq(
              createSearchField(first, SearchFieldDataType.BOOLEAN)
            )
          )
        }
      }
    }
  }
}
