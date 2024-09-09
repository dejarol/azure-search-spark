package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.DataTypes

class SparkInternalConvertersSpec
  extends BasicSpec
    with FieldFactory {

  private lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")

  describe(`object`[SparkInternalConverters.type ]) {
    describe(SHOULD) {
      describe("retrieve a converter for atomic fields with same name and") {
        it("same type") {

          SparkInternalConverters.safeConverterFor(
            createStructField(first, DataTypes.StringType),
            createSearchField(first, SearchFieldDataType.STRING)
          ) shouldBe defined
        }

        it("compatible type") {

          SparkInternalConverters.safeConverterFor(
            createStructField(first, DataTypes.DateType),
            createSearchField(first, SearchFieldDataType.DATE_TIME_OFFSET)
          ) shouldBe defined
        }
      }

      describe("collection fields with same name and") {
        it("same inner type") {

          SparkInternalConverters.safeConverterFor(
            createArrayField(second, DataTypes.IntegerType),
            createCollectionField(second, SearchFieldDataType.INT32)
          ) shouldBe defined
        }

        it("compatible inner type") {

          SparkInternalConverters.safeConverterFor(
            createArrayField(third, DataTypes.StringType),
            createCollectionField(third, SearchFieldDataType.DATE_TIME_OFFSET)
          ) shouldBe defined
        }
      }

      describe("complex fields with same name and") {
        it("same number of subfields of same type") {

          SparkInternalConverters.safeConverterFor(
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
          ) shouldBe defined
        }

        it("same number of subfields of compatible type") {

          SparkInternalConverters.safeConverterFor(
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
          ) shouldBe defined
        }

        it("different number of subfields with same or compatible type") {

          SparkInternalConverters.safeConverterFor(
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
          ) shouldBe defined
        }
      }

      describe("geo fields with same name and") {
        it("same number of subfields with same or compatible types") {

          SparkInternalConverters.safeConverterFor(
            createStructField(fourth, GeoPointRule.GEO_POINT_DEFAULT_STRUCT),
            createSearchField(fourth, SearchFieldDataType.GEOGRAPHY_POINT)
          ) shouldBe defined
        }

        it("different number of subfields with same or compatible types") {

          SparkInternalConverters.safeConverterFor(
            createStructField(fourth,
              createStructType(
                createStructField("type", DataTypes.StringType)
              )
            ),
            createSearchField(fourth, SearchFieldDataType.GEOGRAPHY_POINT)
          ) shouldBe defined
        }
      }
    }

    describe(SHOULD_NOT) {
      describe("retrieve a converter for") {
        it("atomic fields with same type but different name") {

          SparkInternalConverters.safeConverterFor(
            createStructField(first, DataTypes.StringType),
            createSearchField(second, SearchFieldDataType.STRING)
          ) shouldBe empty
        }

        describe("collection fields with") {

          it("different name") {

            SparkInternalConverters.safeConverterFor(
              createArrayField(first, DataTypes.StringType),
              createCollectionField(second, SearchFieldDataType.STRING)
            ) shouldBe empty
          }

          it("incompatible inner type") {

            SparkInternalConverters.safeConverterFor(
              createArrayField(second, DataTypes.StringType),
              createCollectionField(second, SearchFieldDataType.INT64)
            ) shouldBe empty
          }
        }

        describe("complex fields with") {
          it("different name") {

            SparkInternalConverters.safeConverterFor(
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
            ) shouldBe empty
          }

          it("different subfields names") {

            SparkInternalConverters.safeConverterFor(
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
            ) shouldBe empty
          }

          it("incompatible subfield types") {

            SparkInternalConverters.safeConverterFor(
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
            ) shouldBe empty
          }
        }
      }
    }
  }
}
