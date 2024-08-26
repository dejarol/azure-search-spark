package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.schema.SearchFieldTypeOperations
import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, BasicSpec}
import org.scalatest.Inspectors

class AtomicInferSchemaRulesSpec
  extends BasicSpec
    with Inspectors {

  private lazy val atomicSearchTypes: Set[SearchFieldDataType] = SearchFieldTypeOperations.ATOMIC_TYPES
  private lazy val nonAtomicTypes: Seq[SearchFieldDataType] = Seq(
    SearchFieldDataType.COMPLEX,
    SearchFieldDataType.collection(SearchFieldDataType.STRING),
    SearchFieldDataType.GEOGRAPHY_POINT
  )

  describe(`object`[AtomicInferSchemaRules.type ]) {
    describe(SHOULD) {
      describe("retrieve the atomic inferred type") {
        it("safely") {

          forAll(atomicSearchTypes) {
            `type` =>
              AtomicInferSchemaRules.safeInferredTypeOf(`type`) shouldBe defined
          }

          forAll(nonAtomicTypes) {
            `type` =>
              AtomicInferSchemaRules.safeInferredTypeOf(`type`) shouldBe empty
          }
        }

        it("unsafely") {

          forAll(atomicSearchTypes) {
            `type` =>
              noException shouldBe thrownBy {
                AtomicInferSchemaRules.unsafeInferredTypeOf(`type`)
              }
          }

          forAll(nonAtomicTypes) {
            `type` =>
              an[AzureSparkException] shouldBe thrownBy {
                AtomicInferSchemaRules.unsafeInferredTypeOf(`type`)
              }
          }
        }
      }
    }
  }
}
