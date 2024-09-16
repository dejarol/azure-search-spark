package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.schema.SearchFieldTypeOperations
import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, BasicSpec}
import org.apache.spark.sql.types.DataTypes
import org.scalatest.Inspectors

class AtomicTypeConversionRulesSpec
  extends BasicSpec
    with Inspectors {

  private lazy val atomicSearchTypes: Set[SearchFieldDataType] = SearchFieldTypeOperations.ATOMIC_TYPES
  private lazy val nonAtomicTypes: Seq[SearchFieldDataType] = Seq(
    SearchFieldDataType.COMPLEX,
    SearchFieldDataType.collection(SearchFieldDataType.STRING),
    SearchFieldDataType.GEOGRAPHY_POINT
  )

  describe(`object`[AtomicTypeConversionRules.type ]) {
    describe(SHOULD) {
      describe("retrieve the atomic inferred type") {
        it("safely") {

          forAll(atomicSearchTypes) {
            `type` =>
              AtomicTypeConversionRules.safeInferredTypeOf(`type`) shouldBe defined
          }

          forAll(nonAtomicTypes) {
            `type` =>
              AtomicTypeConversionRules.safeInferredTypeOf(`type`) shouldBe empty
          }
        }

        it("unsafely") {

          forAll(atomicSearchTypes) {
            `type` =>
              noException shouldBe thrownBy {
                AtomicTypeConversionRules.unsafeInferredTypeOf(`type`)
              }
          }

          forAll(nonAtomicTypes) {
            `type` =>
              an[AzureSparkException] shouldBe thrownBy {
                AtomicTypeConversionRules.unsafeInferredTypeOf(`type`)
              }
          }
        }
      }

      it("evaluate the existence of a conversion rule") {

        AtomicTypeConversionRules.existsConversionRuleFor(
          DataTypes.DateType,
          SearchFieldDataType.DATE_TIME_OFFSET
        ) shouldBe true

        AtomicTypeConversionRules.existsConversionRuleFor(
          DataTypes.TimestampType,
          SearchFieldDataType.DATE_TIME_OFFSET
        ) shouldBe false
      }
    }
  }
}
