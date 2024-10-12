package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.SchemaViolation.Type
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{SafeCodecSupplierSpec, SchemaViolationsMixins}
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String

import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime}
import scala.reflect.ClassTag

class EncodingSupplierSpec
  extends SafeCodecSupplierSpec
    with SchemaViolationsMixins {

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

  private def assertAtomicCodecExists[TInput, TOutput: ClassTag](
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
      describe("return an atomic encoder for reading") {
        it("string fields as strings") {

          assertAtomicCodecExists[String, UTF8String](
            SearchFieldDataType.STRING,
            DataTypes.StringType,
            "hello",
            UTF8String.fromString
          )
        }

        describe("numeric fields as") {
          it("strings") {

            assertAtomicCodecExists[Integer, UTF8String](
              SearchFieldDataType.INT32,
              DataTypes.StringType,
              123,
              v => UTF8String.fromString(
                String.valueOf(v)
              )
            )

            assertAtomicCodecExists[Long, UTF8String](
              SearchFieldDataType.INT64,
              DataTypes.StringType,
              123,
              v => UTF8String.fromString(
                String.valueOf(v)
              )
            )

            assertAtomicCodecExists[Double, UTF8String](
              SearchFieldDataType.DOUBLE,
              DataTypes.StringType,
              3.14,
              v => UTF8String.fromString(
                String.valueOf(v)
              )
            )

            assertAtomicCodecExists[Float, UTF8String](
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

              assertAtomicCodecExists[Int, Int](
                SearchFieldDataType.INT32,
                DataTypes.IntegerType,
                123,
                identity
              )
            }

            it("different type") {

              assertAtomicCodecExists[Int, Long](
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

            assertAtomicCodecExists[Boolean, Boolean](
              SearchFieldDataType.BOOLEAN,
              DataTypes.BooleanType,
              false,
              identity
            )
          }

          it("strings") {
            assertAtomicCodecExists[Boolean, UTF8String](
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

            assertAtomicCodecExists[String, UTF8String](
              SearchFieldDataType.DATE_TIME_OFFSET,
              DataTypes.StringType,
              OffsetDateTime.now().format(Constants.DATETIME_OFFSET_FORMATTER),
              v => UTF8String.fromString(v)
            )
          }

          it("dates") {

            val offsetDateTime = OffsetDateTime.now()
            assertAtomicCodecExists[String, Int](
              SearchFieldDataType.DATE_TIME_OFFSET,
              DataTypes.DateType,
              offsetDateTime.format(Constants.DATETIME_OFFSET_FORMATTER),
              v => OffsetDateTime.parse(v, Constants.DATETIME_OFFSET_FORMATTER).toLocalDate.toEpochDay.toInt
            )
          }

          it("timestamps") {

            val offsetDateTime = OffsetDateTime.now()
            assertAtomicCodecExists[String, Long](
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
            isForIncompatibleType(head) shouldBe true
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
            head.getType shouldBe Type.INCOMPATIBLE_COMPLEX_FIELD
            isForComplexField(head) shouldBe true
            val maybeViolations = maybeSubFieldViolations(head)
            maybeViolations shouldBe defined
            val subViolations = maybeViolations.get
            subViolations should have size 1
            subViolations.head.getType shouldBe Type.MISSING_FIELD
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
            head.getType shouldBe Type.INCOMPATIBLE_COMPLEX_FIELD
            isForComplexField(head) shouldBe true
            val maybeSubViolations = maybeSubFieldViolations(head)
            maybeSubViolations shouldBe defined
            val subViolations = maybeSubViolations.get
            subViolations should have size 1
            subViolations.head.getType shouldBe Type.INCOMPATIBLE_TYPE
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
            isForArrayField(head) shouldBe true
            val subtypeViolation = maybeSubViolation(head)
            subtypeViolation shouldBe defined
            subtypeViolation.get.getType shouldBe Type.INCOMPATIBLE_TYPE
          }
        }
      }
    }
  }
}
