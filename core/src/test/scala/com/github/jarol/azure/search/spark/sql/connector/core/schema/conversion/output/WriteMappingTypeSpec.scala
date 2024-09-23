package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{GeoPointRule, MappingTypeSpec}
import org.apache.spark.sql.types.{DataTypes, StructField}

class WriteMappingTypeSpec
  extends MappingTypeSpec[StructField, WriteConverter] {

  describe(`object`[WriteMappingType.type ]) {
    describe(SHOULD) {
      describe("retrieve a non-empty converter for") {
        describe("atomic fields with") {
          it("same type") {

            assertDefinedAndAnInstanceOf[AtomicWriteConverters.StringConverter.type](
              WriteMappingType.converterForAtomicTypes(
                DataTypes.StringType,
                SearchFieldDataType.STRING
              )
            )
          }

          it("compatible type") {

            assertDefinedAndAnInstanceOf[AtomicWriteConverters.DateToDatetimeConverter.type](
              WriteMappingType.converterForAtomicTypes(
                DataTypes.DateType,
                SearchFieldDataType.DATE_TIME_OFFSET
              )
            )
          }
        }
      }

      describe("provide a") {
        it("collection converter") {

          WriteMappingType.collectionConverter(
            DataTypes.BooleanType,
            createSearchField(first, SearchFieldDataType.BOOLEAN),
            AtomicWriteConverters.BooleanConverter
          ) shouldBe a[ArrayConverter]
        }

        it("complex converter") {

          WriteMappingType.complexConverter(
            Map.empty
          ) shouldBe a[StructTypeConverter]
        }

        it("geopoint converter") {

          WriteMappingType.geoPointConverter shouldBe GeoPointRule.writeConverter()
        }
      }

      it("retrieve the name from a StructField") {

        val field = createStructField(first, DataTypes.TimestampType)
        WriteMappingType.keyOf(field) shouldBe field
      }
    }
  }
}
