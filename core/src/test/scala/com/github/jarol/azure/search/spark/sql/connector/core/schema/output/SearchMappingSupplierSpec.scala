package com.github.jarol.azure.search.spark.sql.connector.core.schema.output

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.SearchMappingSupplier
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{GeoPointRule, MappingSupplierSpec}
import org.apache.spark.sql.types.{DataTypes, StructField}

class SearchMappingSupplierSpec
  extends MappingSupplierSpec[StructField, SearchPropertyConverter](SearchMappingSupplier) {

  describe(`object`[SearchMappingSupplier.type ]) {
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

      describe("return a Left when there are") {
        it("missing schema fields") {

          assertMappingIsLeft(
            Seq(createStructField(first, DataTypes.StringType)),
            Seq(createSearchField(second, SearchFieldDataType.STRING))
          )
        }

        it("incompatible fields") {

          assertMappingIsLeft(
            Seq(createStructField(first, DataTypes.StringType)),
            Seq(createSearchField(first, SearchFieldDataType.INT32))
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

