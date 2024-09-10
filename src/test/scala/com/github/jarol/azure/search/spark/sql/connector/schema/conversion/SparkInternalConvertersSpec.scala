package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.{DataTypes, StructField}

class SparkInternalConvertersSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")

  /**
   * Assert the existence of a [[SparkInternalConverter]] for given Spark and Search field
   * @param structField Spark field
   * @param searchField Search field
   * @param shouldExists true for converter that should exist
   */

  private def assertConverterExistence(structField: StructField,
                                       searchField: SearchField,
                                       shouldExists: Boolean): Unit = {

    val maybeConverter = SparkInternalConverters.safeConverterFor(
      structField, searchField
    )

    if (shouldExists) {
      maybeConverter shouldBe defined
    } else {
      maybeConverter shouldBe empty
    }
  }

  /**
   * Assert that no converter exists for given Spark and Search fields
   * @param structField Spark field
   * @param searchField Search field
   */

  private def assertNoConverterExists(structField: StructField, searchField: SearchField): Unit = {

    assertConverterExistence(
      structField,
      searchField,
      shouldExists = false
    )
  }

  /**
   * Assert that a converter exists for given Spark and Search fields
   * @param structField Spark field
   * @param searchField Search field
   */

  private def assertConverterExists(structField: StructField, searchField: SearchField): Unit = {

    assertConverterExistence(
      structField,
      searchField, shouldExists = true
    )
  }

  describe(`object`[SparkInternalConverters.type ]) {
    describe(SHOULD) {
      describe("retrieve a converter for atomic fields with same name and") {
        it("same type") {

          assertConverterExists(
            createStructField(first, DataTypes.StringType),
            createSearchField(first, SearchFieldDataType.STRING)
          )
        }

        it("compatible type") {

          assertConverterExists(
            createStructField(first, DataTypes.DateType),
            createSearchField(first, SearchFieldDataType.DATE_TIME_OFFSET)
          )
        }
      }

      describe("collection fields with same name and") {
        it("same inner type") {

          assertConverterExists(
            createArrayField(second, DataTypes.IntegerType),
            createCollectionField(second, SearchFieldDataType.INT32)
          )
        }

        it("compatible inner type") {

          assertConverterExists(
            createArrayField(third, DataTypes.StringType),
            createCollectionField(third, SearchFieldDataType.DATE_TIME_OFFSET)
          )
        }
      }

      describe("complex fields with same name and") {
        it("same number of subfields of same type") {

          assertConverterExists(
            createStructField(
              first,
              createStructType(
                createStructField(second, DataTypes.BooleanType),
                createStructField(third, DataTypes.FloatType)
              )
            ),
            createComplexField(
              first,
              Seq(
                createSearchField(second, SearchFieldDataType.BOOLEAN),
                createSearchField(third, SearchFieldDataType.SINGLE)
              )
            )
          )
        }

        it("same number of subfields of compatible type") {

          assertConverterExists(
            createStructField(
              first,
              createStructType(
                createStructField(second, DataTypes.DateType),
                createStructField(third, DataTypes.StringType)
              )
            ),
            createComplexField(
              first,
              Seq(
                createSearchField(second, SearchFieldDataType.DATE_TIME_OFFSET),
                createSearchField(third, SearchFieldDataType.DATE_TIME_OFFSET)
              )
            )
          )
        }

        it("different number of subfields with same or compatible type") {

          assertConverterExists(
            createStructField(
              first,
              createStructType(
                createStructField(second, DataTypes.BooleanType),
                createStructField(third, DataTypes.DateType)
              )
            ),
            createComplexField(
              first,
              Seq(
                createSearchField(second, SearchFieldDataType.BOOLEAN),
                createSearchField(third, SearchFieldDataType.DATE_TIME_OFFSET),
                createSearchField(fourth, SearchFieldDataType.STRING)
              )
            )
          )
        }
      }

      describe("geo fields with same name and") {
        it("same number of subfields with same or compatible types") {

          assertConverterExists(
            createStructField(fourth, GeoPointRule.GEO_POINT_DEFAULT_STRUCT),
            createSearchField(fourth, SearchFieldDataType.GEOGRAPHY_POINT)
          )
        }

        it("different number of subfields with same or compatible types") {

          assertConverterExists(
            createStructField(fourth,
              createStructType(
                createStructField("type", DataTypes.StringType)
              )
            ),
            createSearchField(fourth, SearchFieldDataType.GEOGRAPHY_POINT)
          )
        }
      }
    }

    describe(SHOULD_NOT) {
      describe("retrieve a converter for") {
        it("atomic fields with same type but different name") {

          assertNoConverterExists(
            createStructField(first, DataTypes.StringType),
            createSearchField(second, SearchFieldDataType.STRING)
          )
        }

        describe("collection fields with") {

          it("different name") {

            assertNoConverterExists(
              createArrayField(first, DataTypes.StringType),
              createCollectionField(second, SearchFieldDataType.STRING)
            )
          }

          it("incompatible inner type") {

            assertNoConverterExists(
              createArrayField(second, DataTypes.StringType),
              createCollectionField(second, SearchFieldDataType.INT64)
            )
          }
        }

        describe("complex fields with") {
          it("different name") {

            assertNoConverterExists(
              createStructField(
                first,
                createStructType(
                  createStructField(second, DataTypes.TimestampType)
                )
              ),
              createComplexField(
                third,
                Seq(
                  createSearchField(second, SearchFieldDataType.DATE_TIME_OFFSET)
                )
              )
            )
          }

          it("different subfields names") {

            assertNoConverterExists(
              createStructField(
                first,
                createStructType(
                  createStructField(second, DataTypes.TimestampType)
                )
              ),
              createComplexField(
                first,
                Seq(
                  createSearchField(third, SearchFieldDataType.DATE_TIME_OFFSET)
                )
              )
            )
          }

          it("incompatible subfield types") {

            assertNoConverterExists(
              createStructField(
                first,
                createStructType(
                  createStructField(second, DataTypes.TimestampType)
                )
              ),
              createComplexField(
                first,
                Seq(
                  createSearchField(second, SearchFieldDataType.STRING)
                )
              )
            )
          }
        }
      }
    }
  }
}
