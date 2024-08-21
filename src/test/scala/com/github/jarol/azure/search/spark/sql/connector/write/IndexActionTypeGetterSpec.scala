package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, BasicSpec}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class IndexActionTypeGetterSpec
  extends BasicSpec {

  private lazy val (actionColName, dateColName) = ("action", "date")
  private lazy val schema = StructType(
    Seq(
      StructField(actionColName, DataTypes.StringType),
      StructField(dateColName, DataTypes.DateType)
    )
  )

  describe(`object`[IndexActionTypeGetter]) {
    describe(SHOULD) {
      describe(s"throw an ${nameOf[AzureSparkException]} for") {

        it("a non existing column") {

          an[AzureSparkException] shouldBe thrownBy {
            IndexActionTypeGetter("hello", schema, IndexActionType.UPLOAD)
          }
        }

        it("a non-string column") {

          an[AzureSparkException] shouldBe thrownBy {
            IndexActionTypeGetter(dateColName, schema, IndexActionType.UPLOAD)
          }
        }
      }
    }
  }

  describe(anInstanceOf[IndexActionTypeGetter]) {
    describe(SHOULD) {
      describe("retrieve the index action type") {
        it("when not null") {

          val action = IndexActionType.DELETE
          val row = InternalRow(UTF8String.fromString(action.name()))
          IndexActionTypeGetter(actionColName, schema, IndexActionType.UPLOAD)(row) shouldBe action
        }

        it("using a default value when null") {

          val (action, default): (UTF8String, IndexActionType) = (null, IndexActionType.DELETE)
          val row = InternalRow(action)
          IndexActionTypeGetter(actionColName, schema, default)(row) shouldBe default
        }
      }
    }
  }
}
