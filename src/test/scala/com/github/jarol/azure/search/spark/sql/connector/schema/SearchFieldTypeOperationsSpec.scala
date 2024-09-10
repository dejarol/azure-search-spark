package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, BasicSpec, FieldFactory}
import org.scalatest.Inspectors

class SearchFieldTypeOperationsSpec
  extends BasicSpec
    with FieldFactory
      with Inspectors {

  /**
   * Run a set of tests using an instance of [[SearchFieldTypeAssertion]]
   * @param assertion assertion to run
   */

  private def runTypeAssertion(assertion: SearchFieldTypeAssertion): Unit = {

    // Check for simple types
    forAll(SearchFieldTypeOperations.ATOMIC_TYPES) {
      k => assertion.predicate(k) shouldBe assertion.expectedAtomic
    }

    // Checks for collection, complex and geo types
    assertion.predicate(SearchFieldDataType.collection(SearchFieldDataType.INT32)) shouldBe assertion.expectedCollection
    assertion.predicate(SearchFieldDataType.COMPLEX) shouldBe assertion.expectedComplex
    assertion.predicate(SearchFieldDataType.GEOGRAPHY_POINT) shouldBe assertion.expectedGeoPoint
  }

  describe(anInstanceOf[SearchFieldTypeOperations]) {
    describe(SHOULD) {
      describe("evaluate if a search type") {
        it("is a string") {

          SearchFieldDataType.STRING.isString shouldBe true
          SearchFieldDataType.INT32.isString shouldBe false
        }

        it("is a number") {

          SearchFieldDataType.STRING.isNumber shouldBe false
          SearchFieldDataType.INT32.isNumber shouldBe true
          SearchFieldDataType.INT64.isNumber shouldBe true
          SearchFieldDataType.DOUBLE.isNumber shouldBe true
          SearchFieldDataType.SINGLE.isNumber shouldBe true
        }

        it("is boolean") {

          SearchFieldDataType.STRING.isBoolean shouldBe false
          SearchFieldDataType.BOOLEAN.isBoolean shouldBe true
        }

        it("is datetime") {

          SearchFieldDataType.INT32.isDateTime shouldBe false
          SearchFieldDataType.DATE_TIME_OFFSET.isDateTime shouldBe true
        }

        it("is atomic") {

          runTypeAssertion(TypeAssertions.Atomic)
        }

        it("is complex") {

          runTypeAssertion(TypeAssertions.Complex)
        }

        it("is a collection") {

          runTypeAssertion(TypeAssertions.Collection)
        }

        it("is a geo point") {

          runTypeAssertion(TypeAssertions.GeoPoint)
        }
      }

      it("extract a collection inner type") {

        // Simple type
        val nonCollectionField = SearchFieldDataType.INT32
        nonCollectionField.safelyExtractCollectionType shouldBe empty
        an[AzureSparkException] shouldBe thrownBy {
          nonCollectionField.unsafelyExtractCollectionType
        }

        // Collection type
        val expectedInnerType = SearchFieldDataType.DATE_TIME_OFFSET
        val collectionType = createCollectionType(expectedInnerType)
        collectionType.safelyExtractCollectionType shouldBe Some(expectedInnerType)
        collectionType.unsafelyExtractCollectionType shouldBe expectedInnerType
      }
    }
  }
}
