package io.github.dejarol.azure.search.spark.connector.core.schema

import io.github.dejarol.azure.search.spark.connector.core.{BasicSpec, DataTypeException, FieldFactory}
import org.apache.spark.sql.types.DataTypes

class SparkTypeOperationsSpec
  extends BasicSpec
    with FieldFactory {

  describe(anInstanceOf[SparkTypeOperations]) {
    describe(SHOULD) {
      describe("evaluate if a Spark type is") {
        it("a string") {

          DataTypes.StringType.isString shouldBe true
          DataTypes.IntegerType.isString shouldBe false
        }

        it("a number") {

          DataTypes.StringType.isNumeric shouldBe false
          DataTypes.IntegerType.isNumeric shouldBe true
          DataTypes.LongType.isNumeric shouldBe true
          DataTypes.DoubleType.isNumeric shouldBe true
          DataTypes.FloatType.isNumeric shouldBe false
        }

        it("a boolean") {

          DataTypes.TimestampType.isBoolean shouldBe false
          DataTypes.BooleanType.isBoolean shouldBe true
        }

        it("a date or timestamp") {

          DataTypes.LongType.isDateTime shouldBe false
          DataTypes.DateType.isDateTime shouldBe true
          DataTypes.TimestampType.isDateTime shouldBe true
        }

        it("atomic") {

          DataTypes.StringType.isAtomic shouldBe true
          DataTypes.IntegerType.isAtomic shouldBe true
          DataTypes.BooleanType.isAtomic shouldBe true
          DataTypes.DateType.isAtomic shouldBe true
        }

        it("a collection") {

          DataTypes.IntegerType.isCollection shouldBe false
          createArrayType(
            DataTypes.IntegerType
          ).isCollection shouldBe true
        }

        it("a struct") {

          DataTypes.IntegerType.isCollection shouldBe false
          createStructType(
            createStructField("name", DataTypes.StringType)
          ).isComplex shouldBe true
        }
      }

      describe("extract both safely and unsafely") {
        it("the collection inner type") {

          val innerType = DataTypes.DateType
          val collectionType = createArrayType(innerType)
          collectionType.safeCollectionInnerType shouldBe Some(innerType)
          collectionType.unsafeCollectionInnerType shouldBe innerType

          val nonCollectionType = DataTypes.BooleanType
          nonCollectionType.safeCollectionInnerType shouldBe empty
          a[DataTypeException] shouldBe thrownBy {
            nonCollectionType.unsafeCollectionInnerType
          }
        }

        it("the type subfields") {

          val subFields = Seq(
            createStructField("first", DataTypes.StringType),
            createStructField("second", DataTypes.DateType)
          )

          val structType = createStructType(subFields: _*)
          structType.safeSubFields shouldBe defined
          structType.safeSubFields.get should contain theSameElementsAs subFields
          structType.unsafeSubFields should contain theSameElementsAs subFields

          DataTypes.StringType.safeSubFields shouldBe empty
          a [DataTypeException] shouldBe thrownBy {
            DataTypes.StringType.unsafeSubFields
          }
        }
      }
    }
  }
}
