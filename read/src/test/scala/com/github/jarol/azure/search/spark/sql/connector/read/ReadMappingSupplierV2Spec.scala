package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.MappingViolation.ViolationType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.MappingViolations
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.MappingViolations.{IncompatibleNestedField, IncompatibleType}
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.DataTypes
import org.scalatest.EitherValues

class ReadMappingSupplierV2Spec
  extends BasicSpec
    with FieldFactory
      with EitherValues {

  private lazy val indexName = "people"
  private lazy val (first, second, third) = ("first", "second", "third")

  describe(`object`[ReadMappingSupplierV2.type ]) {
    describe(SHOULD) {
      describe("return a Right for") {

      }

      describe("return a Left when") {
        describe("some top-level schema fields") {
          it("miss") {

            val result = ReadMappingSupplierV2.build(
              Seq(createStructField(first, DataTypes.StringType)),
              Seq.empty
            ).left.value

            result should have size 1
            val head = result.head
            head.getViolationType shouldBe ViolationType.MISSING_FIELD
            head shouldBe a [MappingViolations.MissingField]
            head.asInstanceOf[MappingViolations.MissingField].getFieldName shouldBe first
          }

          it("have incompatible dtypes") {

            val result = ReadMappingSupplierV2.build(
              Seq(createStructField(first, DataTypes.StringType)),
              Seq(createSearchField(first, SearchFieldDataType.collection(SearchFieldDataType.STRING)))
            ).left.value

            result should have size 1
            val head = result.head
            head.getViolationType shouldBe ViolationType.INCOMPATIBLE_TYPE
            head shouldBe a [IncompatibleType]
          }
        }

        describe("some nested fields") {
          it("miss") {

            val result = ReadMappingSupplierV2.build(
              Seq(
                createStructField(first, createStructType(
                  createStructField(second, DataTypes.StringType))
                )
              ),
              Seq(
                createComplexField(first, Seq(
                  createSearchField(third, SearchFieldDataType.STRING)
                ))
              )
            ).left.value

            result should have size 1
            val head = result.head
            head.getViolationType shouldBe ViolationType.INCOMPATIBLE_NESTED_FIELD
            head shouldBe a[IncompatibleNestedField]
            val casted = head.asInstanceOf[IncompatibleNestedField]
            casted.getSubFieldViolations should have size 1
            casted.getSubFieldViolations.get(0).getViolationType shouldBe ViolationType.MISSING_FIELD
          }

          it("have incompatible dtypes") {

            val result = ReadMappingSupplierV2.build(
              Seq(
                createStructField(first, createStructType(
                  createStructField(second, DataTypes.StringType))
                )
              ),
              Seq(
                createComplexField(first, Seq(
                  createSearchField(second, SearchFieldDataType.collection(
                    SearchFieldDataType.INT32
                  ))
                ))
              )
            ).left.value

            result should have size 1
            val head = result.head
            head.getViolationType shouldBe ViolationType.INCOMPATIBLE_NESTED_FIELD
            head shouldBe a[IncompatibleNestedField]
            val casted = head.asInstanceOf[IncompatibleNestedField]
            casted.getSubFieldViolations should have size 1
            casted.getSubFieldViolations.get(0).getViolationType shouldBe ViolationType.INCOMPATIBLE_TYPE
          }
        }

        describe("some collection fields") {
          it("have incompatible inner type") {

          }
        }
      }
    }
  }
}
