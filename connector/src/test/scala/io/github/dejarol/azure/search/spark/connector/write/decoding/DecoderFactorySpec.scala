package io.github.dejarol.azure.search.spark.connector.write.decoding

import com.azure.search.documents.indexes.models.SearchFieldDataType
import io.github.dejarol.azure.search.spark.connector.core.Constants
import io.github.dejarol.azure.search.spark.connector.core.codec.{CodecErrors, CodecFactorySpec}
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import scala.reflect.ClassTag

class DecoderFactorySpec
  extends CodecFactorySpec {

  private lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")

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

    val maybeDecoder = DecoderFactory.atomicCodecFor(dataType, searchType)
    maybeDecoder shouldBe defined
    val output = maybeDecoder.get.apply(input)
    output shouldBe a [OutputType]
    output shouldBe expectedFunction(input)
  }

  describe(`object`[DecoderFactory.type ]) {
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

            assertAtomicDecoderExists[Long, Integer](
              DataTypes.LongType,
              SearchFieldDataType.INT32,
              123,
              _.intValue()
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

      describe("return a Right for") {
        it("perfectly matching schemas") {

          DecoderFactory.buildComplexCodecInternalMapping(
            createStructType(
              createStructField(first, DataTypes.StringType),
              createStructField(second, DataTypes.IntegerType)
            ),
            Seq(
              createSearchField(first, SearchFieldDataType.STRING),
              createSearchField(second, SearchFieldDataType.INT32)
            )
          ) shouldBe 'right
        }

        describe("matching schemas with different column order") {
          it("for top-level fields") {

            DecoderFactory.buildComplexCodecInternalMapping(
              createStructType(
                createStructField(second, DataTypes.IntegerType),
                createStructField(first, DataTypes.StringType)
              ),
              Seq(
                createSearchField(first, SearchFieldDataType.STRING),
                createSearchField(second, SearchFieldDataType.INT32)
              )
            ) shouldBe 'right
          }

          it("for nested subfields") {

            DecoderFactory.buildComplexCodecInternalMapping(
              createStructType(
                createStructField(second, DataTypes.IntegerType),
                createStructField(first,
                  createStructType(
                    createStructField(third, DataTypes.TimestampType),
                    createStructField(fourth, DataTypes.BooleanType)
                  )
                )
              ),
              Seq(
                createComplexField(first,
                  Seq(
                    createSearchField(fourth, SearchFieldDataType.BOOLEAN),
                    createSearchField(third, SearchFieldDataType.DATE_TIME_OFFSET)
                  )
                ),
                createSearchField(second, SearchFieldDataType.INT32)
              )
            ) shouldBe 'right
          }
        }
      }

      describe("return a Left when") {
        describe("some fields") {
          it("miss") {

            val result = DecoderFactory.buildComplexCodecInternalMapping(
              createStructType(createStructField(first, DataTypes.StringType)),
              Seq(createSearchField(second, SearchFieldDataType.STRING))
            ).left.value

            result shouldBe complex
            val internal = result.internal
            internal should contain key first
            internal(first) shouldBe CodecErrors.forMissingField()
          }
        }

        it("have incompatible dtypes") {

          val (sparkType, searchType) = (
            DataTypes.StringType,
            SearchFieldDataType.collection(SearchFieldDataType.DATE_TIME_OFFSET)
          )
          val result = DecoderFactory.buildComplexCodecInternalMapping(
            createStructType(createStructField(first, sparkType)),
            Seq(createSearchField(first, searchType))
          ).left.value

          result shouldBe complex
          val internal = result.internal
          internal should contain key first
          internal(first) shouldBe CodecErrors.forIncompatibleTypes(sparkType, searchType)
        }

        describe("some collection fields") {
          it("have incompatible inner type") {

            val (sparkInnerType, searchInnerType) = (
              DataTypes.DateType,
              SearchFieldDataType.COMPLEX
            )

            val result = DecoderFactory.buildComplexCodecInternalMapping(
              createStructType(
                createArrayField(first, sparkInnerType)
              ),
              Seq(
                createCollectionField(first, searchInnerType)
              )
            ).left.value

            result shouldBe complex
            val internal = result.internal
            internal should contain key first
            internal(first) shouldBe CodecErrors.forIncompatibleTypes(sparkInnerType, searchInnerType)
          }
        }
      }
    }
  }
}
