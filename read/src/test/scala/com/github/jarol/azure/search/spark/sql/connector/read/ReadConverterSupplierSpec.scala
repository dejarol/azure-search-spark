package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input._
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{GeoPointRule, SafeConverterSupplierSpec}
import org.apache.spark.sql.types.DataTypes

class ReadConverterSupplierSpec
  extends SafeConverterSupplierSpec[ReadConverter](ReadConverterSupplier) {

  describe(`object`[ReadConverterSupplier.type]) {
    describe(SHOULD) {
      describe("return a non-empty converter only for") {
        describe("atomic fields with same name and") {
          it("same type") {

            assertConverterExistsAndIsA[ReadTransformConverter.UTF8_STRING.type](
              createStructField(first, DataTypes.StringType),
              createSearchField(first, SearchFieldDataType.STRING)
            )
          }

          it("compatible type") {

            assertConverterExistsAndIsA[ReadTimeConverter.DATE.type](
              createStructField(first, DataTypes.DateType),
              createSearchField(first, SearchFieldDataType.DATE_TIME_OFFSET)
            )
          }
        }

        describe("collection fields with same name and") {
          it("same inner type") {

            assertConverterExistsAndIsA[CollectionConverter](
              createArrayField(second, DataTypes.IntegerType),
              createCollectionField(second, SearchFieldDataType.INT32)
            )
          }

          it("compatible inner type") {

            assertConverterExistsAndIsA[CollectionConverter](
              createArrayField(third, DataTypes.StringType),
              createCollectionField(third, SearchFieldDataType.DATE_TIME_OFFSET)
            )
          }

          it("complex inner type") {

            assertConverterExistsAndIsA[CollectionConverter](
              createArrayField(
                first,
                createStructType(
                  createStructField(second, DataTypes.StringType),
                  createStructField(third, DataTypes.BooleanType)
                )
              ),
              createComplexCollectionField(
                first,
                createSearchField(second, SearchFieldDataType.STRING),
                createSearchField(third, SearchFieldDataType.BOOLEAN)
              )
            )
          }
        }

        describe("complex fields with same name and") {
          it("same number of subfields of same type") {

            assertConverterExistsAndIsA[ComplexConverter](
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

            assertConverterExistsAndIsA[ComplexConverter](
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

            assertConverterExistsAndIsA[ComplexConverter](
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

            assertConverterExistsAndIsA[ComplexConverter](
              createStructField(fourth, GeoPointRule.GEO_POINT_DEFAULT_STRUCT),
              createSearchField(fourth, SearchFieldDataType.GEOGRAPHY_POINT)
            )
          }

          it("different number of subfields with same or compatible types") {

            assertConverterExistsAndIsA[ComplexConverter](
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
