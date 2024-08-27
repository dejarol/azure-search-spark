package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.GeoPointRule
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, JavaScalaConverters, SearchFieldFactory}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StructField, StructType}
import org.scalatest.Inspectors

class SchemaUtilsSpec
  extends BasicSpec
    with SearchFieldFactory
      with Inspectors {

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
              SearchFieldDataType.collection(
                innerType
              )
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
          val complexField = createSearchField("complex", SearchFieldDataType.COMPLEX)
          complexField.setFields(
            JavaScalaConverters.seqToList(
              innerFields
            )
          )

          SchemaUtils.inferSparkTypeOf(
            complexField
          ) shouldBe StructType(
            innerFields.map(
              SchemaUtils.asStructField
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

          val complexField = createSearchField("complexField", SearchFieldDataType.COMPLEX)
          complexField.setFields(
            JavaScalaConverters.seqToList(innerFields)
          )

          val searchFields = Seq(
            createSearchField("stringField", SearchFieldDataType.STRING),
            createSearchField("collectionField", SearchFieldDataType.collection(
              SearchFieldDataType.INT32)
            ),
            complexField
          )

          val schema = SchemaUtils.asStructType(searchFields)
          schema should have size searchFields.size
          schema should contain theSameElementsAs searchFields.map(
            SchemaUtils.asStructField
          )
        }
      }

      describe("evaluate Spark type compatibilities") {
        it("for atomic types") {

          SchemaUtils.areNaturallyCompatible(DataTypes.StringType, DataTypes.StringType) shouldBe true
          SchemaUtils.areNaturallyCompatible(DataTypes.StringType, DataTypes.IntegerType) shouldBe false
        }

        it("for collection types") {

          SchemaUtils.areNaturallyCompatible(
            ArrayType(DataTypes.StringType, containsNull = true),
            ArrayType(DataTypes.StringType, containsNull = true)
          ) shouldBe true

          SchemaUtils.areNaturallyCompatible(
            ArrayType(DataTypes.StringType, containsNull = true),
            ArrayType(DataTypes.IntegerType, containsNull = true)
          ) shouldBe false
        }

        it("for struct types") {

          val (stringFieldName, intFieldName) = ("string", "int")
          val firstSchema = StructType(
            Seq(
              StructField(stringFieldName, DataTypes.StringType),
              StructField(intFieldName, DataTypes.IntegerType)
            )
          )

          // same names, different types
          val secondSchema = StructType(
            Seq(
              StructField(stringFieldName, DataTypes.DateType),
              StructField(intFieldName, DataTypes.IntegerType)
            )
          )

          // different names, same dtypes
          val thirdSchema = StructType(
            Seq(
              StructField(stringFieldName, DataTypes.StringType),
              StructField("hello", DataTypes.IntegerType)
            )
          )

          SchemaUtils.areNaturallyCompatible(firstSchema, firstSchema) shouldBe true
          SchemaUtils.areNaturallyCompatible(firstSchema, secondSchema) shouldBe false
          SchemaUtils.areNaturallyCompatible(firstSchema, thirdSchema) shouldBe false
        }
      }
    }
  }
}
