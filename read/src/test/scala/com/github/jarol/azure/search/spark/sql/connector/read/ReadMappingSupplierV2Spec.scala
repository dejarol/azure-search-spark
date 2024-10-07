package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.MappingViolation.ViolationType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.MappingViolations._
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{EitherValues, Inspectors}

import scala.reflect.ClassTag

class ReadMappingSupplierV2Spec
  extends BasicSpec
    with FieldFactory
      with EitherValues
        with Inspectors {

  private lazy val (first, second, third) = ("first", "second", "third")

  /**
   * Assert that a converter between two atomic types exists and reads data correctly
   * @param searchType Search type
   * @param dataType Spark type
   * @param value test value
   * @param transform expected input-output transformation
   * @tparam TInput input type
   * @tparam TOutput output type
   */

  private def assertAtomicMappingExists[TInput, TOutput: ClassTag](
                                                                    searchType: SearchFieldDataType,
                                                                    dataType: DataType,
                                                                    value: TInput,
                                                                    transform: TInput => TOutput
                                                                  ): Unit = {

    val result = ReadMappingSupplierV2.forAtomicTypes(dataType, searchType)
    result shouldBe defined

    val output = result.get.apply(value)
    output shouldBe a[TOutput]
    output shouldBe transform(value)
  }

  describe(`object`[ReadMappingSupplierV2.type ]) {
    describe(SHOULD) {
      describe("return a converter for reading") {
        describe("strings from") {
          it("string fields") {

            assertAtomicMappingExists[String, UTF8String](
              SearchFieldDataType.STRING,
              DataTypes.StringType,
              "hello",
              UTF8String.fromString
            )
          }

          it("numeric fields") {

            assertAtomicMappingExists[Integer, UTF8String](
              SearchFieldDataType.INT32,
              DataTypes.StringType,
              123,
              v => UTF8String.fromString(
                String.valueOf(v)
              )
            )

            assertAtomicMappingExists[Long, UTF8String](
              SearchFieldDataType.INT64,
              DataTypes.StringType,
              123,
              v => UTF8String.fromString(
                String.valueOf(v)
              )
            )

            assertAtomicMappingExists[Double, UTF8String](
              SearchFieldDataType.DOUBLE,
              DataTypes.StringType,
              3.14,
              v => UTF8String.fromString(
                String.valueOf(v)
              )
            )

            assertAtomicMappingExists[Float, UTF8String](
              SearchFieldDataType.SINGLE,
              DataTypes.StringType,
              3.14f,
              v => UTF8String.fromString(
                String.valueOf(v)
              )
            )
          }

          it("boolean fields") {

           assertAtomicMappingExists[Boolean, UTF8String](
             SearchFieldDataType.BOOLEAN,
             DataTypes.StringType,
             false,
             v => UTF8String.fromString(
               String.valueOf(v)
             )
           )
          }
        }

        describe("numbers from numeric fields") {
          it("of same type") {

            assertAtomicMappingExists[Integer, Integer](
              SearchFieldDataType.INT32,
              DataTypes.IntegerType,
              123,
              identity
            )
          }

          it("of different type") {

            assertAtomicMappingExists[Int, Long](
              SearchFieldDataType.INT32,
              DataTypes.LongType,
              123,
              _.toLong
            )
          }
        }

        it("booleans from boolean fields") {

          assertAtomicMappingExists[Boolean, Boolean](
            SearchFieldDataType.BOOLEAN,
            DataTypes.BooleanType,
            false,
            identity
          )
        }
      }

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
            head shouldBe a [MissingField]
            head.asInstanceOf[MissingField].getFieldName shouldBe first
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
