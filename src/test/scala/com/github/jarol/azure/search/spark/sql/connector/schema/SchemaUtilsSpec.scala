package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.{BasicSpec, JavaScalaConverters}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.scalatest.Inspectors

class SchemaUtilsSpec
  extends BasicSpec
    with Inspectors {

  /**
   * Run a set of tests using an instance of [[SearchFieldTypeAssertion]]
   * @param assertion assertion to run
   */

  private def runTypeAssertion(assertion: SearchFieldTypeAssertion): Unit = {

    // Check for simple types
    forAll(SchemaUtils.SIMPLE_TYPES.keys) {
      k => assertion.predicate(k) shouldBe assertion.expectedSimple
    }

    // Checks for collection, complex and geo types
    assertion.predicate(SearchFieldDataType.collection(SearchFieldDataType.INT32)) shouldBe assertion.expectedCollection
    assertion.predicate(SearchFieldDataType.COMPLEX) shouldBe assertion.expectedComplex
    assertion.predicate(SearchFieldDataType.GEOGRAPHY_POINT) shouldBe assertion.expectedGeoPoint
  }

  describe(`object`[SchemaUtils.type ]) {
    describe(SHOULD) {
      describe("detect if a search type") {
        it("is simple") {

          runTypeAssertion(TypeAssertions.Simple)
        }

        it("is complex") {

          runTypeAssertion(TypeAssertions.Complex)
        }

        it("is a collection") {

          runTypeAssertion(TypeAssertions.Collection)
        }
      }

      it("extract a collection inner type") {

        // Simple type
        SchemaUtils.safelyExtractCollectionType(SearchFieldDataType.STRING) shouldBe empty
        an[IllegalStateException] shouldBe thrownBy {
          SchemaUtils.unsafelyExtractCollectionType(SearchFieldDataType.STRING)
        }

        // Collection type
        val expectedInnerType = SearchFieldDataType.DATE_TIME_OFFSET
        val collectionType = SearchFieldDataType.collection(expectedInnerType)
        SchemaUtils.safelyExtractCollectionType(collectionType) shouldBe Some(expectedInnerType)
        SchemaUtils.unsafelyExtractCollectionType(collectionType) shouldBe expectedInnerType
      }

      describe("resolve Spark dataType for") {
        it("a simple type") {

          forAll(SchemaUtils.SIMPLE_TYPES.toSeq) {
            case (k, v) =>
              SchemaUtils.sparkDataTypeOf(
                new SearchField("simple", k)
              ) shouldBe v
          }
        }

        it("a collection type") {

          val innerType = SearchFieldDataType.INT64
          SchemaUtils.sparkDataTypeOf(
            new SearchField("collection",
              SearchFieldDataType.collection(
                innerType
              )
            )
          ) shouldBe ArrayType(
            SchemaUtils.sparkDataTypeOf(
              new SearchField("inner", innerType)
            )
          )
        }

        it("a complex type") {

          val innerFields = Seq(
            new SearchField("date", SearchFieldDataType.DATE_TIME_OFFSET),
            new SearchField("flag", SearchFieldDataType.BOOLEAN)
          )
          val complexField = new SearchField("complex", SearchFieldDataType.COMPLEX)
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
      }

      describe("convert a collection of search fields to a schema") {
        it("in the standard scenario") {

          val innerFields = Seq(
            new SearchField("date", SearchFieldDataType.DATE_TIME_OFFSET),
            new SearchField("flag", SearchFieldDataType.BOOLEAN)
          )

          val complexField = new SearchField("complexField", SearchFieldDataType.COMPLEX)
          complexField.setFields(
            JavaScalaConverters.seqToList(innerFields)
          )

          val searchFields = Seq(
            new SearchField("stringField", SearchFieldDataType.STRING),
            new SearchField("collectionField", SearchFieldDataType.collection(
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
