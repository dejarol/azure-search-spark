package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.types.conversion.{AtomicInferSchemaRules, GeoPointRule}
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, JavaScalaConverters, SearchFieldFactory}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.scalatest.Inspectors

class SchemaUtilsSpec
  extends BasicSpec
    with SearchFieldFactory
      with Inspectors {

  describe(`object`[SchemaUtils.type ]) {
    describe(SHOULD) {
      describe("resolve Spark dataType for") {
        it("a simple type") {

          forAll(AtomicInferSchemaRules.allRules().filter(_.useForSchemaInference())) {
            rule =>
              SchemaUtils.sparkDataTypeOf(
                createField("simple", rule.searchType())
              ) shouldBe rule.sparkType()
          }
        }

        it("a collection type") {

          val innerType = SearchFieldDataType.INT64
          SchemaUtils.sparkDataTypeOf(
            createField(
              "collection",
              SearchFieldDataType.collection(
                innerType
              )
            )
          ) shouldBe ArrayType(
            SchemaUtils.sparkDataTypeOf(
              createField("inner", innerType)
            )
          )
        }

        it("a complex type") {

          val innerFields = Seq(
            createField("date", SearchFieldDataType.DATE_TIME_OFFSET),
            createField("flag", SearchFieldDataType.BOOLEAN)
          )
          val complexField = createField("complex", SearchFieldDataType.COMPLEX)
          complexField.setFields(
            JavaScalaConverters.seqToList(
              innerFields
            )
          )

          SchemaUtils.sparkDataTypeOf(
            complexField
          ) shouldBe StructType(
            innerFields.map(
              SchemaUtils.asStructField
            )
          )
        }

        it(" a geo point") {

          SchemaUtils.sparkDataTypeOf(
            createField("location", SearchFieldDataType.GEOGRAPHY_POINT)
          ) shouldBe GeoPointRule.sparkType
        }
      }

      describe("convert a collection of search fields to a schema") {
        it("in the standard scenario") {

          val innerFields = Seq(
            createField("date", SearchFieldDataType.DATE_TIME_OFFSET),
            createField("flag", SearchFieldDataType.BOOLEAN)
          )

          val complexField = createField("complexField", SearchFieldDataType.COMPLEX)
          complexField.setFields(
            JavaScalaConverters.seqToList(innerFields)
          )

          val searchFields = Seq(
            createField("stringField", SearchFieldDataType.STRING),
            createField("collectionField", SearchFieldDataType.collection(
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
    }
  }
}
