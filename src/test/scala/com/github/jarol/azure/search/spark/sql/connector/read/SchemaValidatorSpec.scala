package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, SearchFieldFactory, StructFieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructType}

class SchemaValidatorSpec
  extends BasicSpec
    with SearchFieldFactory
      with StructFieldFactory {

  private lazy val (first, second, third) = ("field1", "field2", "field3")

  describe(`object`[SchemaValidator.type ]) {
    describe(SHOULD) {
      describe("evaluate as compatible") {
        it("two atomic fields with same name and compatible type") {

          SchemaValidator.areCompatible(
            createSearchField(first, SearchFieldDataType.STRING),
            createStructField(first, DataTypes.StringType)
          ) shouldBe true

          SchemaValidator.areCompatible(
            createSearchField(first, SearchFieldDataType.DATE_TIME_OFFSET),
            createStructField(first, DataTypes.DateType)
          ) shouldBe true
        }

        it("two collection fields with same name and compatible type") {

          SchemaValidator.areCompatible(
            createCollectionField(first, SearchFieldDataType.INT32),
            createArrayField(first, DataTypes.IntegerType)
          ) shouldBe true

          SchemaValidator.areCompatible(
            createCollectionField(first, SearchFieldDataType.DATE_TIME_OFFSET),
            createArrayField(first, DataTypes.DateType)
          ) shouldBe true

          SchemaValidator.areCompatible(
            createCollectionField(first, SearchFieldDataType.DOUBLE),
            createArrayField(first, DataTypes.IntegerType)
          ) shouldBe false
        }

        describe("two complex fields") {

          lazy val complexField = createComplexField(
            first,
            Seq(
              createSearchField(second, SearchFieldDataType.DOUBLE),
              createSearchField(third, SearchFieldDataType.DATE_TIME_OFFSET)
            )
          )

          it("that have same number of subfields and all are compatible") {

            SchemaValidator.areCompatible(
              complexField,
              createStructField(
                first,
                StructType(
                  Seq(
                    createStructField(second, DataTypes.DoubleType),
                    createStructField(third, DataTypes.TimestampType)
                  )
                )
              )
            ) shouldBe true
          }

          it("when the Spark field has less subfields but all compatible") {

            SchemaValidator.areCompatible(
              complexField,
              createStructField(
                first,
                StructType(
                  Seq(
                    createStructField(third, DataTypes.DateType)
                  )
                )
              )
            ) shouldBe true
          }
        }
      }

      describe("evaluate as non-compatible") {
        it("two atomic fields with different name or non-compatible types") {

          SchemaValidator.areCompatible(
            createSearchField(first, SearchFieldDataType.BOOLEAN),
            createStructField(first, DataTypes.DateType)
          ) shouldBe false

          SchemaValidator.areCompatible(
            createSearchField(second, SearchFieldDataType.BOOLEAN),
            createStructField(third, DataTypes.BooleanType)
          ) shouldBe false
        }

        it("two collection types with different name or non-compatible inner type") {

          SchemaValidator.areCompatible(
            createCollectionField(first, SearchFieldDataType.BOOLEAN),
            createArrayField(second, DataTypes.BooleanType)
          ) shouldBe false

          SchemaValidator.areCompatible(
            createCollectionField(second, SearchFieldDataType.BOOLEAN),
            createArrayField(second, DataTypes.StringType)
          ) shouldBe false
        }

        describe("two complex fields") {
          it("with different names") {

            SchemaValidator.areCompatible(
              createComplexField(
                first,
                Seq(
                  createSearchField(second, SearchFieldDataType.STRING)
                )
              ),
              createStructField(
                third,
                StructType(
                  Seq(
                    createStructField(second, DataTypes.StringType)
                  )
                )
              )
            ) shouldBe false
          }

          it("with more subfields on Spark side") {

            SchemaValidator.areCompatible(
              createComplexField(
                first,
                Seq(
                  createSearchField(second, SearchFieldDataType.STRING)
                )
              ),
              createStructField(
                first,
                StructType(
                  Seq(
                    createStructField(second, DataTypes.StringType),
                    createStructField(third, DataTypes.TimestampType)
                  )
                )
              )
            ) shouldBe false
          }
        }
      }

      describe("validate a Spark schema against a Search index schema returning") {
        it("an empty Option for valid cases") {

          SchemaValidator.validate(
            Seq(
              createStructField(first, DataTypes.StringType),
              createStructField(second, DataTypes.BooleanType)
            ),
            Seq(
              createSearchField(first, SearchFieldDataType.STRING),
              createSearchField(second, SearchFieldDataType.BOOLEAN)
            )
          ) shouldBe empty
        }

        it(s"a ${nameOf[SchemaCompatibilityException]} otherwise") {

          SchemaValidator.validate(
            Seq(
              createStructField(first, DataTypes.StringType),
              createStructField(second, DataTypes.BooleanType)
            ),
            Seq(
              createSearchField(first, SearchFieldDataType.STRING),
              createSearchField(second, SearchFieldDataType.DATE_TIME_OFFSET)
            )
          ) shouldBe defined
        }
      }
    }
  }
}
