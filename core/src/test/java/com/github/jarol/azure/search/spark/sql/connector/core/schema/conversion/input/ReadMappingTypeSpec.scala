package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{GeoPointRule, MappingTypeSpec}
import org.apache.spark.sql.types.DataTypes

class ReadMappingTypeSpec
  extends MappingTypeSpec[String, ReadConverter] {

  describe(`object`[ReadMappingType.type ]) {
    describe(SHOULD) {
      describe("retrieve a non-empty converter for") {
        describe("atomic fields with") {
          it("same type") {

            assertDefinedAndAnInstanceOf[AtomicReadConverters.StringConverter.type](
              ReadMappingType.converterForAtomicTypes(
                DataTypes.StringType,
                SearchFieldDataType.STRING
              )
            )
          }

          it("compatible type") {

            assertDefinedAndAnInstanceOf[AtomicReadConverters.DateTimeToDateConverter.type](
              ReadMappingType.converterForAtomicTypes(
                DataTypes.DateType,
                SearchFieldDataType.DATE_TIME_OFFSET
              )
            )
          }
        }
      }

      describe("provide a") {
        it("collection converter") {

          ReadMappingType.collectionConverter(
            DataTypes.BooleanType,
            createSearchField(first, SearchFieldDataType.BOOLEAN),
            AtomicReadConverters.BooleanConverter
          ) shouldBe a[CollectionConverter]
        }

        it("complex converter") {

          ReadMappingType.complexConverter(
            Map.empty
          ) shouldBe a[ComplexConverter]
        }

        it("geopoint converter") {

          ReadMappingType.geoPointConverter shouldBe GeoPointRule.readConverter()
        }
      }

      it("retrieve the name from a StructField") {

        ReadMappingType.keyOf(
          createStructField(first, DataTypes.TimestampType)
        ) shouldBe first
      }
    }
  }
}
