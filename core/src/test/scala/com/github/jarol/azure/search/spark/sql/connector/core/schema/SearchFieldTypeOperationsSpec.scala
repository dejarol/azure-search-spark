package com.github.jarol.azure.search.spark.sql.connector.core.schema

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, DataTypeException, FieldFactory}
import org.scalatest.Inspectors

class SearchFieldTypeOperationsSpec
  extends BasicSpec
    with FieldFactory
      with Inspectors {

  private lazy val ATOMIC_TYPES = Set(
    SearchFieldDataType.STRING,
    SearchFieldDataType.INT32,
    SearchFieldDataType.INT64,
    SearchFieldDataType.DOUBLE,
    SearchFieldDataType.BOOLEAN,
    SearchFieldDataType.DATE_TIME_OFFSET
  )

  /**
   * Run a set of tests using an instance of [[SearchFieldTypeAssertion]]
   * @param assertion assertion to run
   */

  private def runTypeAssertion(assertion: SearchFieldTypeAssertion): Unit = {

    // Check for simple types
    forAll(ATOMIC_TYPES) {
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
          SearchFieldDataType.SINGLE.isNumeric shouldBe false
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

          runTypeAssertion(SearchFieldTypeAssertion.Atomic)
        }

        it("complex") {

          runTypeAssertion(SearchFieldTypeAssertion.Complex)
        }

        it("a collection") {

          runTypeAssertion(SearchFieldTypeAssertion.Collection)
        }

        it("a geo point") {

          runTypeAssertion(SearchFieldTypeAssertion.GeoPoint)
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

      describe("evaluate if a type") {
        it("is candidate for partitioning") {

          SearchFieldDataType.STRING.isCandidateForPartitioning shouldBe false
          SearchFieldDataType.SINGLE.isCandidateForPartitioning shouldBe false
          forAll(
            Seq(
              SearchFieldDataType.INT32,
              SearchFieldDataType.INT64,
              SearchFieldDataType.DOUBLE,
              SearchFieldDataType.DATE_TIME_OFFSET
            )
          ) {
            tp => tp.isCandidateForPartitioning shouldBe true
          }
        }

        it("is candidate for faceting") {

          SearchFieldDataType.DATE_TIME_OFFSET.isCandidateForFaceting shouldBe false
          forAll(
            Seq(
              SearchFieldDataType.STRING,
              SearchFieldDataType.INT32,
              SearchFieldDataType.INT64,
              SearchFieldDataType.DOUBLE
            )
          ) {
            tp => tp.isCandidateForFaceting shouldBe true
          }
        }
      }
    }
  }
}
