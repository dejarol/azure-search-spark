package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.Constants
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{SafeCodecSupplierSpec, SchemaViolation, SchemaViolationsMixins}
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import scala.reflect.ClassTag

class DecodersSupplierSpec
  extends SafeCodecSupplierSpec
    with SchemaViolationsMixins {

  private lazy val (first, second) = ("first", "second")

  /**
   * Assert that an atomic decoder between a Spark type and a Search type exists,
   * and that its behavior matches with an expected function
   * @param dataType Spark type
   * @param searchType Search type
   * @param input input sample
   * @param expectedFunction expected decoding function
   * @tparam InputType Spark internal Java/Scala type
   * @tparam OutputType Search Java/Scala type
   */

  private def assertAtomicDecoderExists[InputType, OutputType: ClassTag](
                                                                          dataType: DataType,
                                                                          searchType: SearchFieldDataType,
                                                                          input: InputType,
                                                                          expectedFunction: InputType => OutputType
                                                                        ): Unit = {

    val maybeDecoder = DecodersSupplier.atomicCodecFor(dataType, searchType)
    maybeDecoder shouldBe defined
    val output = maybeDecoder.get.apply(input)
    output shouldBe a [OutputType]
    output shouldBe expectedFunction(input)
  }

  describe(`object`[DecodersSupplier.type ]) {
    describe(SHOULD) {
      describe("return an atomic decoder for writing") {
        it("string fields as strings") {

          val value = "hello"
          assertAtomicDecoderExists[UTF8String, String](
            DataTypes.StringType,
            SearchFieldDataType.STRING,
            UTF8String.fromString(value),
            _ => value
         )
        }

        it("numeric/boolean fields as strings") {

          assertAtomicDecoderExists[Integer, String](
            DataTypes.IntegerType,
            SearchFieldDataType.STRING,
            123,
            String.valueOf
          )

          assertAtomicDecoderExists[Long, String](
            DataTypes.LongType,
            SearchFieldDataType.STRING,
            123,
            String.valueOf
          )

          assertAtomicDecoderExists[Double, String](
            DataTypes.LongType,
            SearchFieldDataType.STRING,
            3.14,
            String.valueOf
          )

          assertAtomicDecoderExists[Float, String](
            DataTypes.LongType,
            SearchFieldDataType.STRING,
            3.14f,
            String.valueOf
          )

          assertAtomicDecoderExists[Boolean, String](
            DataTypes.BooleanType,
            SearchFieldDataType.STRING,
            true,
            String.valueOf
          )
        }

        it("date/timestamp fields as strings") {

          assertAtomicDecoderExists[Integer, String](
            DataTypes.DateType,
            SearchFieldDataType.STRING,
            LocalDate.now().toEpochDay.toInt,
            v => LocalDate.ofEpochDay(v.toLong)
              .format(DateTimeFormatter.ISO_LOCAL_DATE)
          )

          assertAtomicDecoderExists[Long, String](
            DataTypes.TimestampType,
            SearchFieldDataType.STRING,
            ChronoUnit.MICROS.between(Instant.EPOCH, LocalDateTime.now().toInstant(Constants.UTC_OFFSET)),
            v => Instant.EPOCH.plus(v, ChronoUnit.MICROS)
              .atOffset(Constants.UTC_OFFSET)
              .format(Constants.DATETIME_OFFSET_FORMATTER)
          )
        }

        describe("numeric fields") {
          it("of same type") {

            assertAtomicDecoderExists[Integer, Integer](
              DataTypes.IntegerType,
              SearchFieldDataType.INT32,
              1,
              identity
            )
          }

          it("of different types") {

            assertAtomicDecoderExists[Long, Float](
              DataTypes.LongType,
              SearchFieldDataType.SINGLE,
              123,
              _.floatValue()
            )
          }
        }

        it("boolean fields as booleans") {

          assertAtomicDecoderExists[Boolean, Boolean](
            DataTypes.BooleanType,
            SearchFieldDataType.BOOLEAN,
            true,
            identity
          )
        }

        it("date/timestamp fields as datetime offset fields") {

          val date = LocalDate.now()
          assertAtomicDecoderExists[Integer, String](
            DataTypes.DateType,
            SearchFieldDataType.DATE_TIME_OFFSET,
            date.toEpochDay.toInt,
            v => LocalDate.ofEpochDay(v.toLong).atTime(
              LocalTime.MIDNIGHT
            ).atOffset(Constants.UTC_OFFSET).format(
              Constants.DATETIME_OFFSET_FORMATTER
            )
          )

          val timestamp = LocalDateTime.now().atOffset(Constants.UTC_OFFSET)
          assertAtomicDecoderExists[Long, String](
            DataTypes.TimestampType,
            SearchFieldDataType.DATE_TIME_OFFSET,
            ChronoUnit.MICROS.between(Instant.EPOCH, timestamp.toInstant),
            v => Instant.EPOCH.plus(v, ChronoUnit.MICROS)
              .atOffset(Constants.UTC_OFFSET).format(
                Constants.DATETIME_OFFSET_FORMATTER
              )
          )
        }
      }

      describe("return a Left for") {
        it("missing fields") {

          val violations = DecodersSupplier.get(
            Seq(createStructField(first, DataTypes.StringType)),
            Seq(createSearchField(second, SearchFieldDataType.STRING))
          ).left.value

          violations should have size 1
          violations.head.getType shouldBe SchemaViolation.Type.MISSING_FIELD
        }

        it("namesake fields with incompatible types") {

          val violations = DecodersSupplier.get(
            Seq(createStructField(first, DataTypes.IntegerType)),
            Seq(createSearchField(first, SearchFieldDataType.COMPLEX))
          ).left.value

          violations should have size 1
          violations.head.getType shouldBe SchemaViolation.Type.INCOMPATIBLE_TYPE
        }
      }

      describe("return a Right for") {
        it("matching schemas") {

          DecodersSupplier.get(
            Seq(
              createStructField(first, DataTypes.TimestampType),
              createStructField(second, DataTypes.StringType)
            ),
            Seq(
              createSearchField(first, SearchFieldDataType.DATE_TIME_OFFSET),
              createSearchField(second, SearchFieldDataType.STRING)
            )
          ) shouldBe 'right
        }
      }
    }
  }
}
