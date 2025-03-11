package io.github.dejarol.azure.search.spark.connector.write

import com.azure.search.documents.models.IndexActionType
import io.github.dejarol.azure.search.spark.connector.BasicSpec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.EitherValues

class PerDocumentSupplierSpec
  extends BasicSpec
    with EitherValues {

  private lazy val (actionColName, dateColName) = ("action", "date")
  private lazy val schema = StructType(
    Seq(
      StructField(actionColName, DataTypes.StringType),
      StructField(dateColName, DataTypes.DateType)
    )
  )

  describe(`object`[PerDocumentSupplier]) {
    describe(SHOULD) {
      describe(s"return an exception for") {

        it("a non existing column") {

          PerDocumentSupplier.safeApply("hello", schema, IndexActionType.UPLOAD) shouldBe 'left
        }

        it("a non-string column") {

          PerDocumentSupplier.safeApply(dateColName, schema, IndexActionType.UPLOAD) shouldBe 'left
        }
      }
    }
  }

  describe(anInstanceOf[PerDocumentSupplier]) {
    describe(SHOULD) {
      describe("retrieve the index action type") {
        it("when not null") {

          val action = IndexActionType.DELETE
          val row = InternalRow(UTF8String.fromString(action.name()))
          val either = PerDocumentSupplier.safeApply(actionColName, schema, IndexActionType.UPLOAD)

          either shouldBe 'right
          either.right.get.get(row) shouldBe action
        }

        it("using a default value when null") {

          val (action, default): (UTF8String, IndexActionType) = (null, IndexActionType.DELETE)
          val row = InternalRow(action)
          val either = PerDocumentSupplier.safeApply(actionColName, schema, default)

          either shouldBe 'right
          either.right.get.get(row) shouldBe default
        }
      }
    }
  }
}
