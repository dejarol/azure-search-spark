package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, SearchFieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.scalatest.EitherValues

class SearchScanBuilderSpec
  extends BasicSpec
    with SearchFieldFactory
      with EitherValues {

  private lazy val (index, name, id, date) = ("people", "name", "id", "date")
  private lazy val sparkStringField = StructField(name, DataTypes.StringType)
  private lazy val sparkIntField = StructField(id, DataTypes.IntegerType)
  private lazy val sparkDateField = StructField(date, DataTypes.DateType)
  private lazy val searchStringField = createSearchField(sparkStringField.name, SearchFieldDataType.STRING)

  describe(`object`[SearchScanBuilder]) {
    describe(SHOULD) {
      describe("return an empty Right when") {
        it("all schema fields exist on search index") {

          SearchScanBuilder.allSchemaFieldsExists(
            Seq(sparkStringField),
            Seq(searchStringField),
            index
          ) shouldBe 'right
        }

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
      }

      describe(s"return a ${nameOf[SchemaCompatibilityException]} when") {
        it("some schema fields do not exist on target index") {

          SearchScanBuilder.allSchemaFieldsExists(
            Seq(sparkStringField),
            Seq.empty,
            index
          ) shouldBe 'left

          SearchScanBuilder.allSchemaFieldsExists(
            Seq(sparkStringField, sparkIntField),
            Seq(
              createSearchField(sparkStringField.name, SearchFieldDataType.STRING)
            ),
            index
          ) shouldBe 'left
        }

        it("there are datatype incompatibilities") {

          SearchScanBuilder.allDataTypesAreCompatible(
            Seq(sparkStringField),
            Seq(
              createSearchField(sparkStringField.name, SearchFieldDataType.INT32)
            ),
            index
          ) shouldBe 'left
        }
      }
    }
  }
}
