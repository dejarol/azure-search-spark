package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, DataTypeException, FieldFactory}
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
      describe("evaluate if a search type is") {
        it("a string") {

          SearchFieldDataType.STRING.isString shouldBe true
          SearchFieldDataType.INT32.isString shouldBe false
        }

        it("a number") {

          SearchFieldDataType.STRING.isNumeric shouldBe false
          SearchFieldDataType.INT32.isNumeric shouldBe true
          SearchFieldDataType.INT64.isNumeric shouldBe true
          SearchFieldDataType.DOUBLE.isNumeric shouldBe true
          SearchFieldDataType.SINGLE.isNumeric shouldBe true
        }

        it("boolean") {

          SearchFieldDataType.STRING.isBoolean shouldBe false
          SearchFieldDataType.BOOLEAN.isBoolean shouldBe true
        }

        it("datetime") {

          SearchFieldDataType.INT32.isDateTime shouldBe false
          SearchFieldDataType.DATE_TIME_OFFSET.isDateTime shouldBe true
        }

        it("atomic") {

          runTypeAssertion(TypeAssertions.Atomic)
        }

        it("complex") {

          runTypeAssertion(TypeAssertions.Complex)
        }

        it("a collection") {

          runTypeAssertion(TypeAssertions.Collection)
        }

        it("a geo point") {

          runTypeAssertion(TypeAssertions.GeoPoint)
        }
      }

      it("extract a collection inner type") {

        // Simple type
        val nonCollectionField = SearchFieldDataType.INT32
        nonCollectionField.safeCollectionInnerType shouldBe empty
        a [DataTypeException] shouldBe thrownBy {
          nonCollectionField.unsafeCollectionInnerType
        }

        // Collection type
        val expectedInnerType = SearchFieldDataType.DATE_TIME_OFFSET
        val collectionType = createCollectionType(expectedInnerType)
        collectionType.safeCollectionInnerType shouldBe Some(expectedInnerType)
        collectionType.unsafeCollectionInnerType shouldBe expectedInnerType
      }
    }
  }
}
