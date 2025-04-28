package io.github.dejarol.azure.search.spark.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import io.github.dejarol.azure.search.spark.connector.core.Constants
import io.github.dejarol.azure.search.spark.connector.core.schema.{CodecErrors, CodecFactorySpec}
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String

import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime}
import scala.reflect.ClassTag

class EncodersFactorySpec
  extends CodecFactorySpec {

  private lazy val (first, second, third, fourth) = ("first", "second", "third", "fourth")

  /**
   * Assert that an encoder between two atomic types exists and reads data correctly
   * @param searchType Search type
   * @param dataType Spark type
   * @param value test value
   * @param transform expected input-output transformation
   * @tparam TInput input type
   * @tparam TOutput output type
   */

  private def assertAtomicEncoderExists[TInput, TOutput: ClassTag](
                                                                    searchType: SearchFieldDataType,
                                                                    dataType: DataType,
                                                                    value: TInput,
                                                                    transform: TInput => TOutput
                                                                  ): Unit = {

    val result = EncodersFactory.atomicCodecFor(dataType, searchType)
    result shouldBe defined

    val output = result.get.apply(value)
    output shouldBe a [TOutput]
    output shouldBe transform(value)
  }

  describe(`object`[EncodersFactory.type ]) {
    describe(SHOULD) {
      describe("return an atomic encoder for reading") {
        it("string fields as strings") {

          assertAtomicEncoderExists[String, UTF8String](
            SearchFieldDataType.STRING,
            DataTypes.StringType,
            "hello",
            UTF8String.fromString
          )
        }

        describe("numeric fields as") {
          it("strings") {

            assertAtomicEncoderExists[Integer, UTF8String](
              SearchFieldDataType.INT32,
              DataTypes.StringType,
              123,
              v => UTF8String.fromString(
                String.valueOf(v)
              )
            )

            assertAtomicEncoderExists[Long, UTF8String](
              SearchFieldDataType.INT64,
              DataTypes.StringType,
              123,
              v => UTF8String.fromString(
                String.valueOf(v)
              )
            )

            assertAtomicEncoderExists[Double, UTF8String](
              SearchFieldDataType.DOUBLE,
              DataTypes.StringType,
              3.14,
              v => UTF8String.fromString(
                String.valueOf(v)
              )
            )
          }

          describe("numbers of") {
            it("same type") {

              assertAtomicEncoderExists[Int, Int](
                SearchFieldDataType.INT32,
                DataTypes.IntegerType,
                123,
                identity
              )
            }

            it("different type") {

              assertAtomicEncoderExists[Int, Long](
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

            assertAtomicEncoderExists[Boolean, Boolean](
              SearchFieldDataType.BOOLEAN,
              DataTypes.BooleanType,
              false,
              identity
            )
          }

          it("strings") {

            assertAtomicEncoderExists[Boolean, UTF8String](
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

            assertAtomicEncoderExists[String, UTF8String](
              SearchFieldDataType.DATE_TIME_OFFSET,
              DataTypes.StringType,
              OffsetDateTime.now().format(Constants.DATETIME_OFFSET_FORMATTER),
              v => UTF8String.fromString(v)
            )
          }

          it("dates") {

            val offsetDateTime = OffsetDateTime.now()
            assertAtomicEncoderExists[String, Int](
              SearchFieldDataType.DATE_TIME_OFFSET,
              DataTypes.DateType,
              offsetDateTime.format(Constants.DATETIME_OFFSET_FORMATTER),
              v => OffsetDateTime.parse(v, Constants.DATETIME_OFFSET_FORMATTER).toLocalDate.toEpochDay.toInt
            )
          }

          it("timestamps") {

            val offsetDateTime = OffsetDateTime.now()
            assertAtomicEncoderExists[String, Long](
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
        it("perfectly matching schemas") {

          EncodersFactory.buildComplexCodecInternalMapping(
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

            EncodersFactory.buildComplexCodecInternalMapping(
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

            EncodersFactory.buildComplexCodecInternalMapping(
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

            val result = EncodersFactory.buildComplexCodecInternalMapping(
              createStructType(createStructField(first, DataTypes.StringType)),
              Seq.empty
            ).left.value

            val jsonMap = jObjectFields(result.toJValue)
            jsonMap should contain key first
            jValueAsJSONString(jsonMap(first)) shouldBe codecErrorAsJSONString(CodecErrors.forMissingField())
          }

          it("have incompatible dtypes") {

            val (sparkType, searchType) = (
              DataTypes.StringType,
              SearchFieldDataType.collection(SearchFieldDataType.STRING)
            )

            val result = EncodersFactory.buildComplexCodecInternalMapping(
              createStructType(createStructField(first, sparkType)),
              Seq(createSearchField(first, searchType))
            ).left.value

            val jsonMap = jObjectFields(result.toJValue)
            jsonMap should contain key first
            jValueAsJSONString(jsonMap(first)) shouldBe codecErrorAsJSONString(
              CodecErrors.forIncompatibleTypes(sparkType, searchType)
            )
          }
        }

        describe("some collection fields") {
          it("have incompatible inner type") {

            val (sparkInnerType, searchInnerType) = (
              DataTypes.DateType,
              SearchFieldDataType.COMPLEX
            )

            val result = EncodersFactory.buildComplexCodecInternalMapping(
              createStructType(
                createArrayField(first, sparkInnerType)
              ),
              Seq(
                createCollectionField(first, searchInnerType)
              )
            ).left.value

            val jsonMap = jObjectFields(result.toJValue)
            jsonMap should contain key first
            jValueAsJSONString(jsonMap(first)) shouldBe codecErrorAsJSONString(
              CodecErrors.forIncompatibleTypes(sparkInnerType, searchInnerType)
            )
          }
        }
      }
    }
  }
}
