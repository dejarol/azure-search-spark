package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.BasicSpec
import org.scalatest.Inspectors

class FilterValueFormattersSpec
  extends BasicSpec
    with Inspectors {

  describe(`object`[FilterValueFormatters.type ]) {
    describe(SHOULD) {
      describe("return the proper formatter for") {
        it("strings") {

          FilterValueFormatters.forType(
            SearchFieldDataType.STRING
          ) shouldBe a[FilterValueFormatters.StringFormatter.type ]
        }

        it("numbers") {

          forAll(
            Seq(
              SearchFieldDataType.INT32,
              SearchFieldDataType.INT64,
              SearchFieldDataType.DOUBLE,
              SearchFieldDataType.SINGLE
            )
          ) {
           `type` =>
             FilterValueFormatters.forType(`type`) shouldBe
               a[FilterValueFormatters.NumericFormatter.type ]
          }
        }

        it("datetime") {

          FilterValueFormatters.forType(
            SearchFieldDataType.DATE_TIME_OFFSET
          ) shouldBe a[FilterValueFormatters.DateTimeFormatter.type ]
        }
      }
    }
  }
}
