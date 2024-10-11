package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.SchemaViolation.Type
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.SchemaViolations._
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, Constants, FieldFactory}
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{EitherValues, Inspectors}

import java.time.{Instant, OffsetDateTime}
import java.time.temporal.ChronoUnit
import scala.reflect.ClassTag

class EncodingSupplierSpec
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

    val result = EncodingSupplier.atomicCodecFor(dataType, searchType)
    result shouldBe defined

    val output = result.get.apply(value)
    output shouldBe a[TOutput]
    output shouldBe transform(value)
  }

  describe(`object`[EncodingSupplier.type ]) {
    describe(SHOULD) {
      describe("return an atomic converter for reading") {
        it("string fields as strings") {

          assertAtomicMappingExists[String, UTF8String](
            SearchFieldDataType.STRING,
            DataTypes.StringType,
            "hello",
            UTF8String.fromString
          )
        }

        describe("numeric fields as") {
          it("strings") {

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

          describe("numbers of") {
            it("same type") {

              assertAtomicMappingExists[Int, Int](
                SearchFieldDataType.INT32,
                DataTypes.IntegerType,
                123,
                identity
              )
            }

            it("different type") {

              assertAtomicMappingExists[Int, Long](
                SearchFieldDataType.INT32,
                DataTypes.LongType,
                123,
                _.toLong
              )
            }
          }
        }

        describe("boolean fields as") {
          it("booleans") {

            assertAtomicMappingExists[Boolean, Boolean](
              SearchFieldDataType.BOOLEAN,
              DataTypes.BooleanType,
              false,
              identity
            )
          }

          it("strings") {
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

        describe("datetime fields as") {
          it("strings") {

            assertAtomicMappingExists[String, UTF8String](
              SearchFieldDataType.DATE_TIME_OFFSET,
              DataTypes.StringType,
              OffsetDateTime.now().format(Constants.DATETIME_OFFSET_FORMATTER),
              v => UTF8String.fromString(v)
            )
          }

          it("dates") {

            val offsetDateTime = OffsetDateTime.now()
            assertAtomicMappingExists[String, Int](
              SearchFieldDataType.DATE_TIME_OFFSET,
              DataTypes.DateType,
              offsetDateTime.format(Constants.DATETIME_OFFSET_FORMATTER),
              v => OffsetDateTime.parse(v, Constants.DATETIME_OFFSET_FORMATTER).toLocalDate.toEpochDay.toInt
            )
          }

          it("timestamps") {

            val offsetDateTime = OffsetDateTime.now()
            assertAtomicMappingExists[String, Long](
              SearchFieldDataType.DATE_TIME_OFFSET,
              DataTypes.TimestampType,
              offsetDateTime.format(Constants.DATETIME_OFFSET_FORMATTER),
              v => ChronoUnit.MICROS.between(
                Instant.EPOCH,
                OffsetDateTime.parse(v, Constants.DATETIME_OFFSET_FORMATTER).toInstant
              )
            )
          }
        }
      }

      describe("return a Right for") {
        it("a non-clashing schema") {

          EncodingSupplier.get(
            Seq(
              createStructField(first, DataTypes.StringType),
              createStructField(second, DataTypes.IntegerType)
            ),
            Seq(
              createSearchField(first, SearchFieldDataType.STRING),
              createSearchField(second, SearchFieldDataType.INT32)
            )
          )
        }
      }

      describe("return a Left when") {
        describe("some top-level schema fields") {
          it("miss") {

            val result = EncodingSupplier.get(
              Seq(createStructField(first, DataTypes.StringType)),
              Seq.empty
            ).left.value

            result should have size 1
            val head = result.head
            head.getType shouldBe Type.MISSING_FIELD
            head.getFieldName shouldBe first
          }

          it("have incompatible dtypes") {

            val result = EncodingSupplier.get(
              Seq(createStructField(first, DataTypes.StringType)),
              Seq(createSearchField(first, SearchFieldDataType.collection(SearchFieldDataType.STRING)))
            ).left.value

            result should have size 1
            val head = result.head
            head.getType shouldBe Type.INCOMPATIBLE_TYPE
            head shouldBe a [IncompatibleType]
          }
        }

        describe("some nested fields") {
          it("miss") {

            val result = EncodingSupplier.get(
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
            head.getType shouldBe Type.INCOMPATIBLE_NESTED_FIELD
            head shouldBe a[IncompatibleNestedField]
            val casted = head.asInstanceOf[IncompatibleNestedField]
            casted.getSubFieldViolations should have size 1
            casted.getSubFieldViolations.get(0).getType shouldBe Type.MISSING_FIELD
          }

          it("have incompatible dtypes") {

            val result = EncodingSupplier.get(
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
            head.getType shouldBe Type.INCOMPATIBLE_NESTED_FIELD
            head shouldBe a[IncompatibleNestedField]
            val casted = head.asInstanceOf[IncompatibleNestedField]
            casted.getSubFieldViolations should have size 1
            casted.getSubFieldViolations.get(0).getType shouldBe Type.INCOMPATIBLE_TYPE
          }
        }

        describe("some collection fields") {
          it("have incompatible inner type") {

            val result = EncodingSupplier.get(
              Seq(
                createArrayField(first, DataTypes.DateType)
              ),
              Seq(
                createCollectionField(first, SearchFieldDataType.COMPLEX)
              )
            ).left.value

            result should have size 1
            val head = result.head
            head.getType shouldBe Type.INCOMPATIBLE_ARRAY_TYPE
            head shouldBe a [ArrayViolation]
            val subtypeViolation = head.asInstanceOf[ArrayViolation].getSubtypeViolation
            subtypeViolation.getType shouldBe Type.INCOMPATIBLE_TYPE
          }
        }
      }
    }
  }
}
