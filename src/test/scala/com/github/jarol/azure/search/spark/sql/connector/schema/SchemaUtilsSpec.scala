package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.GeoPointRule
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types._
import org.scalatest.Inspectors

class SchemaUtilsSpec
  extends BasicSpec
    with FieldFactory
      with Inspectors {

  private lazy val (first, second, third) = ("field1", "field2", "field3")

  describe(`object`[SchemaUtils.type ]) {
    describe(SHOULD) {
      describe("resolve Spark dataType for") {
        it("an atomic type") {

          val expected: Map[SearchFieldDataType, DataType] = Map(
            SearchFieldDataType.STRING -> DataTypes.StringType,
            SearchFieldDataType.INT32 -> DataTypes.IntegerType,
            SearchFieldDataType.INT64 -> DataTypes.LongType,
            SearchFieldDataType.DOUBLE -> DataTypes.DoubleType,
            SearchFieldDataType.SINGLE -> DataTypes.FloatType,
            SearchFieldDataType.BOOLEAN -> DataTypes.BooleanType
          )

          forAll(expected.toSeq) {
            case (searchType, expectedSparkType) =>
              SchemaUtils.inferSparkTypeOf(
                createSearchField("fieldName", searchType
                )
              ) shouldBe expectedSparkType
          }
        }

        it("a collection type") {

          val innerType = SearchFieldDataType.INT64
          SchemaUtils.inferSparkTypeOf(
            createSearchField(
              "collection",
              createCollectionType(innerType)
            )
          ) shouldBe ArrayType(
            SchemaUtils.inferSparkTypeOf(
              createSearchField("inner", innerType)
            )
          )
        }

        it("a complex type") {

          val innerFields = Seq(
            createSearchField("date", SearchFieldDataType.DATE_TIME_OFFSET),
            createSearchField("flag", SearchFieldDataType.BOOLEAN)
          )
          val complexField = createComplexField("complex", innerFields)

          SchemaUtils.inferSparkTypeOf(
            complexField
          ) shouldBe StructType(
            innerFields.map(
              SchemaUtils.toStructField
            )
          )
        }

        it(" a geo point") {

          SchemaUtils.inferSparkTypeOf(
            createSearchField("location", SearchFieldDataType.GEOGRAPHY_POINT)
          ) shouldBe GeoPointRule.sparkType
        }
      }

      describe("convert a collection of search fields to a schema") {
        it("in the standard scenario") {

          val innerFields = Seq(
            createSearchField("date", SearchFieldDataType.DATE_TIME_OFFSET),
            createSearchField("flag", SearchFieldDataType.BOOLEAN)
          )

          val complexField = createComplexField("complexField", innerFields)
          val searchFields = Seq(
            createSearchField("stringField", SearchFieldDataType.STRING),
            createCollectionField("collectionField", SearchFieldDataType.INT32),
            complexField
          )

          val schema = SchemaUtils.toStructType(searchFields)
          schema should have size searchFields.size
          schema should contain theSameElementsAs searchFields.map(
            SchemaUtils.toStructField
          )
        }
      }

      it("evaluate if all schema fields exist") {

        val schema = Seq(
          createStructField(first, DataTypes.StringType)
        )

        val firstSetOfSearchFields = Seq(
          createSearchField(first, SearchFieldDataType.INT64)
        )

        val secondSetOfSearchFields = Seq(
          createSearchField(second, SearchFieldDataType.STRING)
        )

        SchemaUtils.allSchemaFieldsExist(schema, firstSetOfSearchFields) shouldBe true
        SchemaUtils.allSchemaFieldsExist(schema, secondSetOfSearchFields) shouldBe false
        SchemaUtils.allSchemaFieldsExist(schema, Seq.empty) shouldBe false
      }

      it("return missing schema fields") {

        val schema = Seq(
          createStructField(first, DataTypes.TimestampType),
          createStructField(third, DataTypes.DateType)
        )

        val searchFields = Seq(
          createSearchField(third, SearchFieldDataType.BOOLEAN)
        )

        val actual = SchemaUtils.getMissingSchemaFields(schema, searchFields)
        val expected = schema.collect {
          case sp if !searchFields.exists {
            se => se.getName.equalsIgnoreCase(sp.name)
          } => sp.name
        }

        actual should have size expected.size
        actual should contain theSameElementsAs expected
      }

      it("match namesake fields") {

        val schema = Seq(
          createStructField(first, DataTypes.StringType),
          createStructField(second, DataTypes.IntegerType)
        )

        val searchFields = Seq(
          createSearchField(first, SearchFieldDataType.COMPLEX)
        )

        val output = SchemaUtils.matchNamesakeFields(schema, searchFields)
        val expectedSize = schema.count {
          sp => searchFields.exists {
            se => sp.name.equalsIgnoreCase(se.getName)
          }
        }

        output.size shouldBe expectedSize
        forAll(output.toSeq) {
          case (k, v) =>
            k.name shouldBe v.getName
        }
      }

      describe("evaluate Spark type compatibilities of") {
        it("atomic types") {

          SchemaUtils.evaluateSparkTypesCompatibility(DataTypes.StringType, DataTypes.StringType) shouldBe true
          SchemaUtils.evaluateSparkTypesCompatibility(DataTypes.StringType, DataTypes.IntegerType) shouldBe false
        }

        it("collection types") {

          SchemaUtils.evaluateSparkTypesCompatibility(
            ArrayType(DataTypes.StringType, containsNull = true),
            ArrayType(DataTypes.StringType, containsNull = true)
          ) shouldBe true

          SchemaUtils.evaluateSparkTypesCompatibility(
            ArrayType(DataTypes.StringType, containsNull = true),
            ArrayType(DataTypes.IntegerType, containsNull = true)
          ) shouldBe false
        }

        it("struct types") {

          val (stringFieldName, intFieldName) = ("string", "int")
          val firstSchema = StructType(
            Seq(
              createStructField(stringFieldName, DataTypes.StringType),
              createStructField(intFieldName, DataTypes.IntegerType)
            )
          )

          // same names, different types
          val secondSchema = StructType(
            Seq(
              createStructField(stringFieldName, DataTypes.DateType),
              createStructField(intFieldName, DataTypes.IntegerType)
            )
          )

          // different names, same dtypes
          val thirdSchema = StructType(
            Seq(
              createStructField(stringFieldName, DataTypes.StringType),
              createStructField("hello", DataTypes.IntegerType)
            )
          )

          SchemaUtils.evaluateSparkTypesCompatibility(firstSchema, firstSchema) shouldBe true
          SchemaUtils.evaluateSparkTypesCompatibility(firstSchema, secondSchema) shouldBe false
          SchemaUtils.evaluateSparkTypesCompatibility(firstSchema, thirdSchema) shouldBe false
        }
      }

      describe("evaluate as compatible") {
        it("two atomic fields with same name and compatible type") {

          SchemaUtils.areCompatibleFields(
            createStructField(first, DataTypes.StringType),
            createSearchField(first, SearchFieldDataType.STRING)
          ) shouldBe true

          SchemaUtils.areCompatibleFields(
            createStructField(first, DataTypes.DateType),
            createSearchField(first, SearchFieldDataType.DATE_TIME_OFFSET)
          ) shouldBe true
        }

        it("two collection fields with same name and compatible type") {

          SchemaUtils.areCompatibleFields(
            createArrayField(first, DataTypes.IntegerType),
            createCollectionField(first, SearchFieldDataType.INT32)
          ) shouldBe true

          SchemaUtils.areCompatibleFields(
            createArrayField(first, DataTypes.DateType),
            createCollectionField(first, SearchFieldDataType.DATE_TIME_OFFSET)
          ) shouldBe true

          SchemaUtils.areCompatibleFields(
            createArrayField(first, DataTypes.IntegerType),
            createCollectionField(first, SearchFieldDataType.DOUBLE)
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

            SchemaUtils.areCompatibleFields(
              createStructField(
                first,
                StructType(
                  Seq(
                    createStructField(second, DataTypes.DoubleType),
                    createStructField(third, DataTypes.TimestampType)
                  )
                )
              ),
              complexField
            ) shouldBe true
          }

          it("when the Spark field has less subfields but all compatible") {

            SchemaUtils.areCompatibleFields(
              createStructField(
                first,
                StructType(
                  Seq(
                    createStructField(third, DataTypes.DateType)
                  )
                )
              ),
              complexField
            ) shouldBe true
          }
        }
      }

      describe("evaluate as non-compatible") {
        it("two atomic fields with different name or non-compatible types") {

          SchemaUtils.areCompatibleFields(
            createStructField(first, DataTypes.DateType),
            createSearchField(first, SearchFieldDataType.BOOLEAN)
          ) shouldBe false

          SchemaUtils.areCompatibleFields(
            createStructField(third, DataTypes.BooleanType),
            createSearchField(second, SearchFieldDataType.BOOLEAN)
          ) shouldBe false
        }

        it("two collection types with different name or non-compatible inner type") {

          SchemaUtils.areCompatibleFields(
            createArrayField(second, DataTypes.BooleanType),
            createCollectionField(first, SearchFieldDataType.BOOLEAN)
          ) shouldBe false

          SchemaUtils.areCompatibleFields(
            createArrayField(second, DataTypes.StringType),
            createCollectionField(second, SearchFieldDataType.BOOLEAN)
          ) shouldBe false
        }

        describe("two complex fields") {
          it("with different names") {

            SchemaUtils.areCompatibleFields(
              createStructField(
                third,
                StructType(
                  Seq(
                    createStructField(second, DataTypes.StringType)
                  )
                )
              ),
              createComplexField(
                first,
                Seq(
                  createSearchField(second, SearchFieldDataType.STRING)
                )
              )
            ) shouldBe false
          }

          it("with more subfields on Spark side") {

            SchemaUtils.areCompatibleFields(
              createStructField(
                first,
                StructType(
                  Seq(
                    createStructField(second, DataTypes.StringType),
                    createStructField(third, DataTypes.TimestampType)
                  )
                )
              ),
              createComplexField(
                first,
                Seq(
                  createSearchField(second, SearchFieldDataType.STRING)
                )
              )
            ) shouldBe false
          }
        }
      }

      describe("retrieve the Search inferred type for a Spark") {
        it("atomic type") {

          SchemaUtils.inferSearchTypeFor(
            createStructField(
              "first",
              DataTypes.TimestampType
            ),
            None
          ) shouldBe SearchFieldDataType.DATE_TIME_OFFSET
        }

        describe("array type") {
          it("with inner atomic type") {

            val inputCollectionType = DataTypes.IntegerType
            SchemaUtils.inferSearchTypeFor(
              createStructField(
                "first",
                inputCollectionType
              ),
              None
            ) shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.INT32
            )
          }

          it("with inner struct type") {

            // TODO
          }
        }
      }
    }
  }
}
