package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.GeoPointType
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, DataTypeException, FieldFactory}
import org.apache.spark.sql.types._
import org.scalatest.Inspectors

import java.util.{List => JList}

class SchemaUtilsSpec
  extends BasicSpec
    with FieldFactory
      with Inspectors {

  private lazy val (first, second, third) = ("field1", "field2", "field3")

  /**
   * Assert that all Search fields exist within the expected field set
   * @param actual actual set of fields
   * @param expected expected set of fields (keys are names, values are data types)
   */

  private def assertExistAll(
                              actual: JList[SearchField],
                              expected: Map[String, SearchFieldDataType]
                            ): Unit = {

    forAll(actual) {
      f => expected.exists {
        case (name, sType) => f.getName.equalsIgnoreCase(name) &&
          f.getType.equals(sType)
      } shouldBe true
    }
  }

  describe(`object`[SchemaUtils.type ]) {
    describe(SHOULD) {
      describe("throw an exception for") {
        it(s"a field whose datatype is ${SearchFieldDataType.SINGLE}") {

          a [DataTypeException] shouldBe thrownBy {
            SchemaUtils.inferSparkTypeOf(
              createSearchField(first, SearchFieldDataType.SINGLE)
            )
          }

          a [DataTypeException] shouldBe thrownBy {
            SchemaUtils.inferSparkTypeOf(
              createComplexField(first,
                Seq(
                  createSearchField(second, SearchFieldDataType.SINGLE)
                )
              )
            )
          }
        }
      }

      describe("resolve Spark dataType for") {
        it("an atomic type") {

          val expected: Map[SearchFieldDataType, DataType] = Map(
            SearchFieldDataType.STRING -> DataTypes.StringType,
            SearchFieldDataType.INT32 -> DataTypes.IntegerType,
            SearchFieldDataType.INT64 -> DataTypes.LongType,
            SearchFieldDataType.DOUBLE -> DataTypes.DoubleType,
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

        describe("a collection type when the inner type is") {
          it(s"${SearchFieldDataType.SINGLE}") {

            SchemaUtils.inferSparkTypeOf(
              createCollectionField(first, SearchFieldDataType.SINGLE)
            ) shouldBe ArrayType(DataTypes.FloatType)
          }

          it("a supported atomic type") {

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

            SchemaUtils.inferSparkTypeOf(
              createComplexCollectionField(
                first,
                createSearchField(second, SearchFieldDataType.DATE_TIME_OFFSET),
                createSearchField(third, SearchFieldDataType.BOOLEAN)
              )
            )
          }
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

        it("a geo point") {

          SchemaUtils.inferSparkTypeOf(
            createSearchField("location", SearchFieldDataType.GEOGRAPHY_POINT)
          ) shouldBe GeoPointType.SCHEMA
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

      describe("retrieve the Search inferred type for a Spark") {
        it("atomic type") {

          SchemaUtils.inferSearchTypeFor(DataTypes.TimestampType) shouldBe SearchFieldDataType.DATE_TIME_OFFSET
        }

        describe("array type") {
          it("with inner atomic type") {

            val inputCollectionType = DataTypes.IntegerType
            SchemaUtils.inferSearchTypeFor(
              createArrayType(inputCollectionType)
            ) shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.INT32
            )
          }

          it("with inner float type") {

            SchemaUtils.inferSearchTypeFor(
              createArrayType(DataTypes.FloatType)
            ) shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.SINGLE
            )
          }

          it("with inner struct type") {

            SchemaUtils.inferSearchTypeFor(
              createArrayType(
                createStructType(
                  createStructField(second, DataTypes.StringType),
                  createStructField(third, DataTypes.DateType)
                )
              )
            ) shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.COMPLEX
            )
          }

          it("with compatible geo point type") {

            SchemaUtils.inferSearchTypeFor(
              createArrayType(GeoPointType.SCHEMA)
            ) shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.GEOGRAPHY_POINT
            )
          }
        }
      }

      describe("convert Struct fields to Search fields") {
        it("atomic types") {

          val structField = createStructField(first, DataTypes.StringType)
          val searchField = SchemaUtils.toSearchField(structField)

          searchField.getName shouldBe structField.name
          searchField.getType shouldBe SearchFieldDataType.STRING
        }

        describe("collection types") {
          it("with atomic inner type") {

            val structField = createArrayField(first, DataTypes.DateType)
            val searchField = SchemaUtils.toSearchField(structField)

            searchField.getName shouldBe structField.name
            searchField.getType shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.DATE_TIME_OFFSET
            )
          }

          it("with inner float type") {

            val structField = createArrayField(first, DataTypes.FloatType)
            val searchField = SchemaUtils.toSearchField(structField)

            searchField.getName shouldBe structField.name
            searchField.getType shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.SINGLE
            )
          }

          it("with complex inner type") {

            val structField = createArrayField(
              first,
              createStructType(
                createStructField(second, DataTypes.IntegerType),
                createStructField(third, DataTypes.BooleanType)
              )
            )

            val searchField = SchemaUtils.toSearchField(structField)
            searchField.getName shouldBe structField.name
            searchField.getType shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.COMPLEX
            )

            assertExistAll(
              searchField.getFields,
              Map(
                second -> SearchFieldDataType.INT32,
                third -> SearchFieldDataType.BOOLEAN
              )
            )
          }

          it("with geo inner type") {

            val structField = createArrayField(first, GeoPointType.SCHEMA)
            val searchField = SchemaUtils.toSearchField(structField)
            searchField.getName shouldBe structField.name
            searchField.getType shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.GEOGRAPHY_POINT
            )

            // subFields should not be added
            searchField.getFields shouldBe null
          }
        }

        it("for struct types") {

          val structType = createStructType(
            createStructField(first, DataTypes.IntegerType),
            createStructField(second, DataTypes.DateType)
          )

          val structField = createStructField(third, structType)
          val searchField = SchemaUtils.toSearchField(structField)

          searchField.getName shouldBe structField.name
          searchField.getType shouldBe SearchFieldDataType.COMPLEX

          assertExistAll(
            searchField.getFields,
            Map(
              first -> SearchFieldDataType.INT32,
              second -> SearchFieldDataType.DATE_TIME_OFFSET
            )
          )
        }

        it("for geo compatible struct types") {

          val structType = GeoPointType.SCHEMA
          val structField = createStructField(third, structType)
          val searchField = SchemaUtils.toSearchField(structField)

          searchField.getName shouldBe structField.name
          searchField.getType shouldBe SearchFieldDataType.GEOGRAPHY_POINT
        }
      }
    }
  }
}
