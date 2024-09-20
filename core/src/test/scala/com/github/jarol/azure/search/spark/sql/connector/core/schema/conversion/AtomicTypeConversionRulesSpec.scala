package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SearchFieldTypeOperations
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, DataTypeException, FieldFactory}
import org.apache.spark.sql.types.DataTypes
import org.scalatest.Inspectors

class AtomicTypeConversionRulesSpec
  extends BasicSpec
    with FieldFactory
      with Inspectors {

  private lazy val atomicSearchTypes: Set[SearchFieldDataType] = SearchFieldTypeOperations.ATOMIC_TYPES
  private lazy val nonAtomicTypes: Seq[SearchFieldDataType] = Seq(
    SearchFieldDataType.COMPLEX,
    SearchFieldDataType.collection(SearchFieldDataType.STRING),
    SearchFieldDataType.GEOGRAPHY_POINT
  )

  describe(`object`[AtomicTypeConversionRules.type ]) {
    describe(SHOULD) {
      describe("retrieve inferred Spark type") {
        it("both safely and unsafely") {

          forAll(atomicSearchTypes) {
            `type` =>
              AtomicTypeConversionRules.safeInferredTypeOf(`type`) shouldBe defined
              noException shouldBe thrownBy {
                AtomicTypeConversionRules.unsafeInferredTypeOf(`type`)
              }
          }

          forAll(nonAtomicTypes) {
            `type` =>
              AtomicTypeConversionRules.safeInferredTypeOf(`type`) shouldBe empty
              a [DataTypeException] shouldBe thrownBy {
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

      describe("retrieve Search inferred type") {
        it("both safely and unsafely") {

          val (dType, expected) = (DataTypes.StringType, SearchFieldDataType.STRING)
          AtomicTypeConversionRules.safeInferredTypeOf(dType) shouldBe Some(expected)
          AtomicTypeConversionRules.unsafeInferredTypeOf(dType) shouldBe expected

          val arrayType = createArrayType(DataTypes.IntegerType)
          AtomicTypeConversionRules.safeInferredTypeOf(arrayType) shouldBe empty

          a [DataTypeException] shouldBe thrownBy {

            AtomicTypeConversionRules.unsafeInferredTypeOf(
              createArrayType(DataTypes.IntegerType)
            )
          }
        }
      }

      describe("safely retrieve the converter between a Spark type and a Search type") {
        it("using inference rules") {

          AtomicTypeConversionRules.safeSparkConverterForTypes(
            DataTypes.StringType,
            SearchFieldDataType.STRING
          ) shouldBe defined
        }

        it("using conversion rules") {

          AtomicTypeConversionRules.safeSparkConverterForTypes(
            DataTypes.DateType,
            SearchFieldDataType.DATE_TIME_OFFSET
          ) shouldBe defined
        }
      }
    }
  }
}
