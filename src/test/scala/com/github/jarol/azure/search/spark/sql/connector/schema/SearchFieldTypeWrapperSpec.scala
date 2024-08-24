package com.github.jarol.azure.search.spark.sql.connector.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.AtomicInferSchemaRules
import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, BasicSpec}
import org.scalatest.Inspectors

import scala.language.implicitConversions

class SearchFieldTypeWrapperSpec
  extends BasicSpec
    with Inspectors {

  /**
   * Run a set of tests using an instance of [[SearchFieldTypeAssertion]]
   * @param assertion assertion to run
   */

  private def runTypeAssertion(assertion: SearchFieldTypeAssertion): Unit = {

    // Check for simple types
    forAll(AtomicInferSchemaRules.allRules().map(_.searchType())) {
      k => assertion.predicate(k) shouldBe assertion.expectedSimple
    }

    // Checks for collection, complex and geo types
    assertion.predicate(SearchFieldDataType.collection(SearchFieldDataType.INT32)) shouldBe assertion.expectedCollection
    assertion.predicate(SearchFieldDataType.COMPLEX) shouldBe assertion.expectedComplex
    assertion.predicate(SearchFieldDataType.GEOGRAPHY_POINT) shouldBe assertion.expectedGeoPoint
  }

  describe(anInstanceOf[SearchFieldTypeWrapper]) {
    describe(SHOULD) {
      describe("evaluate if a search field") {
        it("is simple") {

          runTypeAssertion(TypeAssertions.Simple)
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
        val collectionType = SearchFieldDataType.collection(expectedInnerType)
        collectionType.safelyExtractCollectionType shouldBe Some(expectedInnerType)
        collectionType.unsafelyExtractCollectionType shouldBe expectedInnerType
      }
    }
  }
}
