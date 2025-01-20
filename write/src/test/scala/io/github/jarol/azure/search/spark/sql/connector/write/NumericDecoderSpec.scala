package io.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchFieldDataType
import io.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import org.apache.spark.sql.types.{DataType, DataTypes}

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}

class NumericDecoderSpec
  extends BasicSpec {

  /**
   * Assert the correct decoding behavior
   * @param dataType Spark data type
   * @param searchType Search data type
   * @param input input for numeric decoder
   * @param expectedDecodingFunction expected decoding function
   * @tparam I decoder input type
   * @tparam O decoder output type
   */

  private def assertDecoderBehavior[I, O](
                                           dataType: DataType,
                                           searchType: SearchFieldDataType,
                                           input: I,
                                           expectedDecodingFunction: I => O
                                         ): Unit = {

    val decoder = NumericDecoder(dataType, searchType)
    decoder.apply(input) shouldBe expectedDecodingFunction(input)
    decoder.apply(null.asInstanceOf[I]) shouldBe null
  }

  describe(anInstanceOf[NumericDecoder]) {
    describe(SHOULD) {
      describe("decode a Spark internal") {
        describe("integer to a Search") {
          it("int") {

            assertDecoderBehavior[Integer, Integer](
              DataTypes.IntegerType,
              SearchFieldDataType.INT32,
              1,
              identity
            )
          }

          it("long") {

            assertDecoderBehavior[Integer, JLong](
              DataTypes.IntegerType,
              SearchFieldDataType.INT64,
              123456789,
              _.longValue()
            )
          }

          it("double") {

            assertDecoderBehavior[Integer, Double](
              DataTypes.IntegerType,
              SearchFieldDataType.DOUBLE,
              12345679,
              _.doubleValue()
            )
          }
        }

        describe("long to a Search") {
          it("int") {

            assertDecoderBehavior[JLong, Integer](
              DataTypes.LongType,
              SearchFieldDataType.INT32,
              12345678910L,
              _.intValue()
            )
          }

          it("long") {

            assertDecoderBehavior[JLong, JLong](
              DataTypes.LongType,
              SearchFieldDataType.INT64,
              1234567810L,
              identity
            )
          }

          it("double") {

            assertDecoderBehavior[JLong, JDouble](
              DataTypes.LongType,
              SearchFieldDataType.DOUBLE,
              12345678910L,
              _.doubleValue()
            )
          }
        }

        describe("double to a Search") {
          it("int") {

            assertDecoderBehavior[JDouble, Integer](
              DataTypes.DoubleType,
              SearchFieldDataType.INT32,
              3.14,
              _.intValue()
            )
          }

          it("long") {

            assertDecoderBehavior[JDouble, JLong](
              DataTypes.DoubleType,
              SearchFieldDataType.INT64,
              123.456,
              _.longValue()
            )
          }

          it("double") {

            assertDecoderBehavior[JDouble, JDouble](
              DataTypes.DoubleType,
              SearchFieldDataType.DOUBLE,
              123.456,
              identity
            )
          }
        }

        describe("float to a Search") {
          it("int") {

            assertDecoderBehavior[JFloat, Integer](
              DataTypes.FloatType,
              SearchFieldDataType.INT32,
              3.14f,
              _.intValue()
            )
          }

          it("long") {

            assertDecoderBehavior[JFloat, JLong](
              DataTypes.FloatType,
              SearchFieldDataType.INT64,
              123.456f,
              _.longValue()
            )
          }

          it("double") {

            assertDecoderBehavior[JFloat, JDouble](
              DataTypes.FloatType,
              SearchFieldDataType.DOUBLE,
              123.456f,
              _.doubleValue()
            )
          }
        }
      }
    }
  }
}
