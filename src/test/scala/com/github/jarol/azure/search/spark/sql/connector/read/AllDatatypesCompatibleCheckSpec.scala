package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, SearchFieldFactory}

class AllDatatypesCompatibleCheckSpec
  extends BasicSpec
    with SearchFieldFactory {

  describe(anInstanceOf[AllDatatypesCompatibleCheck]) {
    describe(SHOULD) {
      describe("evaluate data types incompatibilities returning") {
        it("an empty Option for valid cases") {

          // TODO
          /*
          describe("there's no datatype incompatibility") {
          it("in a standard scenario") {

            SearchScanBuilder.allDataTypesAreCompatible(
              Seq(sparkStringField),
              Seq(searchStringField),
              index
            ) shouldBe 'right
          }

          it("between date and datetime offset") {

            SearchScanBuilder.allDataTypesAreCompatible(
              Seq(sparkDateField),
              Seq(
                createSearchField(sparkDateField.name, SearchFieldDataType.DATE_TIME_OFFSET)
              ),
              index
            ) shouldBe 'right
          }
        }
           */

        }

        it(s"a ${nameOf[SchemaCompatibilityException]} otherwise") {

          // TODO
          /*
          SearchScanBuilder.allDataTypesAreCompatible(
            Seq(sparkStringField),
            Seq(
              createSearchField(sparkStringField.name, SearchFieldDataType.INT32)
            ),
            index
          ) shouldBe 'left
           */
        }
      }
    }
  }
}
