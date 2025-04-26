package io.github.dejarol.azure.search.spark.connector.core.schema

import io.github.dejarol.azure.search.spark.connector.core.DataTypeException
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField}

class StructFieldOperationsSpec
  extends BasicSpec
    with FieldFactory {

  /**
   * Create an anonymous field (for tests where field name is useless)
   * @param `type` field type
   * @return a [[org.apache.spark.sql.types.StructField]]
   */

  private def createAnonymousField(`type`: DataType): StructField = createStructField("hello", `type`)

  describe(anInstanceOf[StructFieldOperations]) {
    describe(SHOULD) {
      describe("evaluate if a Spark field is") {
        it("a string") {

          createAnonymousField(DataTypes.StringType).isString shouldBe true
          createAnonymousField(DataTypes.IntegerType).isString shouldBe false
        }

        it("a number") {

          createAnonymousField(DataTypes.StringType).isNumeric shouldBe false
          createAnonymousField(DataTypes.IntegerType).isNumeric shouldBe true
          createAnonymousField(DataTypes.LongType).isNumeric shouldBe true
          createAnonymousField(DataTypes.DoubleType).isNumeric shouldBe true
          createAnonymousField(DataTypes.FloatType).isNumeric shouldBe false
        }

        it("a boolean") {

          createAnonymousField(DataTypes.TimestampType).isBoolean shouldBe false
          createAnonymousField(DataTypes.BooleanType).isBoolean shouldBe true
        }

        it("a datetime") {

          createAnonymousField(DataTypes.LongType).isDateTime shouldBe false
          createAnonymousField(DataTypes.DateType).isDateTime shouldBe true
          createAnonymousField(DataTypes.TimestampType).isDateTime shouldBe true
        }

        it("atomic") {

          createAnonymousField(DataTypes.StringType).isAtomic shouldBe true
          createAnonymousField(DataTypes.IntegerType).isAtomic shouldBe true
          createAnonymousField(DataTypes.BooleanType).isAtomic shouldBe true
          createAnonymousField(DataTypes.DateType).isAtomic shouldBe true
        }

        it("a collection") {

          createAnonymousField(DataTypes.IntegerType).isCollection shouldBe false
          createAnonymousField(
            createArrayType(
              DataTypes.IntegerType
            )
          ).isCollection shouldBe true
        }

        it("a struct") {

          createAnonymousField(DataTypes.IntegerType).isCollection shouldBe false
          createAnonymousField(
            createStructType(
              createStructField("name", DataTypes.StringType)
            )
          ).isComplex shouldBe true
        }
      }

      describe("extract both safely and unsafely") {
        it("the collection inner type") {

          val innerType = DataTypes.DateType
          val collectionField = createAnonymousField(
            createArrayType(innerType)
          )
          collectionField.safeCollectionInnerType shouldBe Some(innerType)
          collectionField.unsafeCollectionInnerType shouldBe innerType

          val nonCollectionField = createAnonymousField(DataTypes.BooleanType)
          nonCollectionField.safeCollectionInnerType shouldBe empty
          a[DataTypeException] shouldBe thrownBy {
            nonCollectionField.unsafeCollectionInnerType
          }
        }

        it("the subfields") {

          val subFields = Seq(
            createStructField("first", DataTypes.StringType),
            createStructField("second", DataTypes.DateType)
          )

          val complexField = createAnonymousField(
            createStructType(subFields: _*)
          )
          complexField.safeSubFields shouldBe defined
          complexField.safeSubFields.get should contain theSameElementsAs subFields
          complexField.unsafeSubFields should contain theSameElementsAs subFields

          val nonComplexField = createAnonymousField(DataTypes.StringType)
          nonComplexField.safeSubFields shouldBe empty
          a [DataTypeException] shouldBe thrownBy {
            nonComplexField.unsafeSubFields
          }
        }
      }
    }
  }
}
