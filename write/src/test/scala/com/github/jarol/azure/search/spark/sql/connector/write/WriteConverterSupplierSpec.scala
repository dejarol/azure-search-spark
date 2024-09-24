package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.{ArrayConverter, AtomicWriteConverters, StructTypeConverter, WriteConverter}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{GeoPointRule, SafeConverterSupplierSpec}
import org.apache.spark.sql.types.DataTypes

class WriteConverterSupplierSpec
  extends SafeConverterSupplierSpec[WriteConverter](WriteConverterSupplier) {

  describe(`object`[WriteConverterSupplier.type]) {
    describe(SHOULD) {
      describe("return a non-empty converter only for") {
        describe("atomic fields with same name and") {
          it("same type") {

            assertConverterExistsAndIsA[AtomicWriteConverters.StringConverter.type](
              createStructField(first, DataTypes.StringType),
              createSearchField(first, SearchFieldDataType.STRING)
            )
          }

          it("compatible type") {

            assertConverterExistsAndIsA[AtomicWriteConverters.DateToDatetimeConverter.type](
              createStructField(first, DataTypes.DateType),
              createSearchField(first, SearchFieldDataType.DATE_TIME_OFFSET)
            )
          }
        }

        describe("collection fields with same name and") {
          it("same inner type") {

            assertConverterExistsAndIsA[ArrayConverter](
              createArrayField(second, DataTypes.IntegerType),
              createCollectionField(second, SearchFieldDataType.INT32)
            )
          }

          it("compatible inner type") {

            assertConverterExistsAndIsA[ArrayConverter](
              createArrayField(third, DataTypes.StringType),
              createCollectionField(third, SearchFieldDataType.DATE_TIME_OFFSET)
            )
          }

          it("complex inner type") {

            assertConverterExistsAndIsA[ArrayConverter](
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

            assertConverterExistsAndIsA[StructTypeConverter](
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

            assertConverterExistsAndIsA[StructTypeConverter](
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

            assertConverterExistsAndIsA[StructTypeConverter](
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

            assertConverterExistsAndIsA[StructTypeConverter](
              createStructField(fourth, GeoPointRule.GEO_POINT_DEFAULT_STRUCT),
              createSearchField(fourth, SearchFieldDataType.GEOGRAPHY_POINT)
            )
          }

          it("different number of subfields with same or compatible types") {

            assertConverterExistsAndIsA[StructTypeConverter](
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
