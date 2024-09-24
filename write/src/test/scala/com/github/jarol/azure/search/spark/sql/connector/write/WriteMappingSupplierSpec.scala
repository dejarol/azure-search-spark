package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.MappingSupplierSpec
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.WriteConverter
import org.apache.spark.sql.types.DataTypes

class WriteMappingSupplierSpec
  extends MappingSupplierSpec[WriteConverter](WriteMappingSupplier) {

  describe(`object`[WriteMappingSupplier.type ]) {
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
