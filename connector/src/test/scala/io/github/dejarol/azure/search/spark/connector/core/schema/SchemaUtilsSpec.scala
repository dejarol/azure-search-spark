package io.github.dejarol.azure.search.spark.connector.core.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import io.github.dejarol.azure.search.spark.connector.{BasicSpec, FieldFactory}
import io.github.dejarol.azure.search.spark.connector.core.{DataTypeException, JavaScalaConverters}
import org.apache.spark.sql.types._

import java.util.{List => JList}

class SchemaUtilsSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val (first, second, third) = ("field1", "field2", "field3")

  /**
   * Create an action that will enable a field feature
   * @param feature feature to enable
   * @return a [[SearchFieldAction]] that will enable a feature
   */

  private def actionForEnablingFeature(feature: SearchFieldFeature): SearchFieldAction = {

    (field: SearchField) => feature.enableOnField(field)
  }

  /**
   * Convert a [[StructField]] to a [[SearchField]]
   * @param structField struct field
   * @param ctx field creation context
   * @return the equivalent Search field
   */

  private def toSearchField(
                             structField: StructField,
                             ctx: SearchFieldCreationContext
                           ): SearchField = {

    SchemaUtils.toSearchField(
      structField, ctx
    )
  }

  /**
   * Convert a [[StructField]] to a [[SearchField]]
   * @param structField struct field
   * @return the equivalent Search field
   */

  private def toSearchField(structField: StructField): SearchField = {

    toSearchField(
      structField, NoOpFieldCreationContext
    )
  }

  /**
   * Convert a [[StructField]] to a [[SearchField]], creating a context filled by given actions
   * @param structField struct field
   * @param actions actions for the context implementation
   * @return the equivalent Search field
   */

  private def toSearchField(
                             structField: StructField,
                             actions: Map[String, SearchFieldAction]
                           ): SearchField = {

    toSearchField(
      structField,
      SearchFieldCreationContextImpl(
        None,
        actions
      )
    )
  }

  /**
   * Convert a [[StructField]] to a [[SearchField]], creating a context filled with given
   * field paths to exclude from natural geo conversion
   * @param structField struct field
   * @param geoExclusions field paths to exclude from natural geo conversion
   * @return the equivalent Search field
   */

  private def toSearchField(
                             structField: StructField,
                             geoExclusions: Seq[String]
                           ): SearchField = {

    toSearchField(
      structField,
      SearchFieldCreationContextImpl(
        Some(geoExclusions),
        Map.empty
      )
    )
  }

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

  /**
   * Assert that a Search field has been marked as complex, and that its subfields match
   * the structure of a GeoPoint
   * @param field a candidate Search field
   */

  private def assertIsComplexWithGeopointStructure(field: SearchField): Unit = {

    assertExistAll(
      field.getFields,
      Map(
        GeoPointType.TYPE_LABEL -> SearchFieldDataType.STRING,
        GeoPointType.COORDINATES_LABEL -> SearchFieldDataType.collection(
          SearchFieldDataType.DOUBLE
        )
      )
    )
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
          ) shouldBe GeoPointType.SPARK_SCHEMA
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
              createArrayType(GeoPointType.SPARK_SCHEMA)
            ) shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.GEOGRAPHY_POINT
            )
          }
        }
      }

      describe("convert Struct fields to Search fields") {
        it("with atomic types") {

          val structField = createStructField(first, DataTypes.StringType)
          val searchField = toSearchField(structField)

          searchField.getName shouldBe structField.name
          searchField.getType shouldBe SearchFieldDataType.STRING
        }

        describe("with collection of") {
          it("atomic types") {

            val structField = createArrayField(first, DataTypes.DateType)
            val searchField = toSearchField(structField)

            searchField.getName shouldBe structField.name
            searchField.getType shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.DATE_TIME_OFFSET
            )
          }

          it("floats") {

            val structField = createArrayField(first, DataTypes.FloatType)
            val searchField = toSearchField(structField)

            searchField.getName shouldBe structField.name
            searchField.getType shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.SINGLE
            )
          }

          it("complex types") {

            val structField = createArrayField(
              first,
              createStructType(
                createStructField(second, DataTypes.IntegerType),
                createStructField(third, DataTypes.BooleanType)
              )
            )

            val searchField = toSearchField(structField)
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

          it("geo points") {

            val structField = createArrayField(first, GeoPointType.SPARK_SCHEMA)
            val searchField = toSearchField(structField)
            searchField.getName shouldBe structField.name
            searchField.getType shouldBe SearchFieldDataType.collection(
              SearchFieldDataType.GEOGRAPHY_POINT
            )

            // subFields should not be added
            searchField.getFields shouldBe null
          }
        }

        it("with complex types") {

          val structType = createStructType(
            createStructField(first, DataTypes.IntegerType),
            createStructField(second, DataTypes.DateType)
          )

          val structField = createStructField(third, structType)
          val searchField = toSearchField(structField)

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

        it("with geo points") {

          val structType = GeoPointType.SPARK_SCHEMA
          val structField = createStructField(third, structType)
          val searchField = toSearchField(structField)

          searchField.getName shouldBe structField.name
          searchField.getType shouldBe SearchFieldDataType.GEOGRAPHY_POINT
        }

        describe("taking into account user-defined specifications, like") {

          describe("enriching Search fields with some special properties, like") {
            describe("enable/disable a feature") {
              it("for top-level fields") {

                val (matchingFieldName, feature) = ("hello", SearchFieldFeature.SEARCHABLE)
                val matchingStructField = createStructField(matchingFieldName, DataTypes.StringType)
                val fieldActions = Map(
                  matchingFieldName -> actionForEnablingFeature(feature)
                )

                val matchingSearchField = toSearchField(matchingStructField, fieldActions)
                matchingSearchField shouldBe enabledFor(feature)

                val nonMatchingStructField = createStructField("world", DataTypes.IntegerType)
                val nonMatchingSearchField = toSearchField(nonMatchingStructField, fieldActions)
                nonMatchingSearchField should not be enabledFor(feature)
              }

              it("for nested fields") {

                val (parentName, matchingFieldName, feature) = ("address", "street", SearchFieldFeature.SEARCHABLE)
                val structField = createStructField(
                  parentName,
                  createStructType(
                    createStructField("city", DataTypes.StringType),
                    createStructField("zipCode", DataTypes.IntegerType),
                    createStructField(matchingFieldName, DataTypes.StringType)
                  )
                )

                val fieldActions = Map(
                  s"$parentName.$matchingFieldName" -> actionForEnablingFeature(feature)
                )

                val searchField = toSearchField(structField, fieldActions)
                val subFields = JavaScalaConverters.listToSeq(searchField.getFields)
                val (matching, nonMatching) = subFields.partition {
                  _.getName.equalsIgnoreCase(matchingFieldName)
                }

                matching should have size 1
                matching.head shouldBe enabledFor(feature)
                forAll(nonMatching) {
                  _ should not be enabledFor(feature)
                }
              }
            }
          }

          describe("geo-conversion exclusions") {

            it("for top-level fields") {

              val candidateGeopointField = createGeopointField("position")
              val searchField = toSearchField(candidateGeopointField, Seq(candidateGeopointField.name))

              // The equivalent Search field should be complex
              searchField.getName shouldBe candidateGeopointField.name
              searchField.getType shouldBe SearchFieldDataType.COMPLEX
              assertIsComplexWithGeopointStructure(searchField)
            }

            it("for nested fields") {

              val (addressFieldName, positionFieldName) = ("address", "position")
              val addressField = createStructField(
                addressFieldName,
                createStructType(
                  createGeopointField(positionFieldName)
                )
              )

              val searchField = toSearchField(addressField, Seq(s"$addressFieldName.$positionFieldName"))
              searchField.getName shouldBe addressFieldName
              searchField.getType shouldBe SearchFieldDataType.COMPLEX

              // The only subfield should be complex
              val subFields = searchField.getFields
              subFields should have size 1
              val positionField = subFields.get(0)
              positionField.getName shouldBe positionFieldName
              searchField.getType shouldBe SearchFieldDataType.COMPLEX
              assertIsComplexWithGeopointStructure(positionField)
            }

            it("for top-level collection fields") {

              val collectionField = createGeopointArray("locations")
              val searchField = toSearchField(collectionField, Seq(collectionField.name))
              searchField.getName shouldBe collectionField.name
              searchField.getType shouldBe createCollectionType(SearchFieldDataType.COMPLEX)
              assertIsComplexWithGeopointStructure(searchField)
            }

            it("for nested collection fields") {

              val (companyFieldName, locationsFieldName) = ("company", "locations")
              val companyField = createStructField(
                companyFieldName,
                createStructType(
                  createGeopointArray(locationsFieldName)
                )
              )
              val searchField = toSearchField(companyField, Seq(s"$companyFieldName.$locationsFieldName"))
              searchField.getName shouldBe companyFieldName
              searchField.getType shouldBe SearchFieldDataType.COMPLEX
              val subFields = searchField.getFields
              subFields should have size 1
              val locationsField = subFields.get(0)
              locationsField.getName shouldBe locationsFieldName
              locationsField.getType shouldBe createCollectionType(SearchFieldDataType.COMPLEX)
              assertIsComplexWithGeopointStructure(locationsField)
            }
          }
        }
      }
    }
  }
}
