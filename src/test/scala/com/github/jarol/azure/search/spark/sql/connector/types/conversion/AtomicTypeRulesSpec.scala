package com.github.jarol.azure.search.spark.sql.connector.types.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, BasicSpec}
import org.scalatest.Inspectors

class AtomicTypeRulesSpec
  extends BasicSpec
    with Inspectors {

  private lazy val atomicSearchTypes: Seq[SearchFieldDataType] = Seq(
    SearchFieldDataType.STRING,
    SearchFieldDataType.INT32,
    SearchFieldDataType.INT64,
    SearchFieldDataType.DOUBLE,
    SearchFieldDataType.SINGLE,
    SearchFieldDataType.BOOLEAN,
    SearchFieldDataType.DATE_TIME_OFFSET
  )

  private lazy val nonAtomicTypes: Seq[SearchFieldDataType] = Seq(
    SearchFieldDataType.COMPLEX,
    SearchFieldDataType.collection(SearchFieldDataType.STRING),
    SearchFieldDataType.GEOGRAPHY_POINT
  )

  describe(`object`[InferSchemaRules.type ]) {
    describe(SHOULD) {
      it("evaluate if an inference rule exists for a search atomic type") {

        forAll(atomicSearchTypes) {
          `type` =>
            InferSchemaRules.existsRuleForType(`type`) shouldBe true
        }

        forAll(nonAtomicTypes) {
          `type` =>
            InferSchemaRules.existsRuleForType(`type`) shouldBe false
        }
      }

      describe("retrieve an atomic conversion rule") {
        it("safely") {

          forAll(atomicSearchTypes) {
            `type` =>
              InferSchemaRules.safeRuleForType(`type`) shouldBe defined
          }

          forAll(nonAtomicTypes) {
            `type` =>
              InferSchemaRules.safeRuleForType(`type`) shouldBe empty
          }
        }

        it("unsafely") {

          forAll(atomicSearchTypes) {
            `type` =>
              noException shouldBe thrownBy {
                InferSchemaRules.unsafeRuleForType(`type`)
              }
          }

          forAll(nonAtomicTypes) {
            `type` =>
              an[AzureSparkException] shouldBe thrownBy {
                InferSchemaRules.unsafeRuleForType(`type`)
              }
          }
        }
      }
    }
  }
}
