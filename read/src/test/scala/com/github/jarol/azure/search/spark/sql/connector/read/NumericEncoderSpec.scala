package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import org.apache.spark.sql.types.{DataType, DataTypes}

import java.lang.{Double => JDouble, Long => JLong}

class NumericEncoderSpec
  extends BasicSpec {

  /**
   * Assert te behavior of a numeric encoder
   * @param searchType encoder's Search type
   * @param dataType encoder's Spark type
   * @param input input value
   * @param expectedEncoding expected encoding function
   * @tparam I input type
   * @tparam O output type
   */

  private def assertEncoderBehavior[I, O](
                                           searchType: SearchFieldDataType,
                                           dataType: DataType,
                                           input: I,
                                           expectedEncoding: I => O
                                         ): Unit = {

    val encoder = NumericEncoder(searchType, dataType)
    encoder.apply(input) shouldBe expectedEncoding(input)
    encoder.apply(null.asInstanceOf[I]) shouldBe null
  }

  describe(anInstanceOf[NumericEncoder]) {
    describe(SHOULD) {
      describe("allow many numeric encodings depending on provided types, like from") {
        describe("int to") {
          it("int") {

            assertEncoderBehavior[Integer, Integer](
              SearchFieldDataType.INT32,
              DataTypes.IntegerType,
              12,
              identity
            )
          }

          it("long") {

            assertEncoderBehavior[Integer, JLong](
              SearchFieldDataType.INT32,
              DataTypes.LongType,
              12345,
              _.longValue()
            )
          }

          it("double") {

            assertEncoderBehavior[Integer, JDouble](
              SearchFieldDataType.INT32,
              DataTypes.DoubleType,
              12345,
              _.doubleValue()
            )
          }
        }

        describe("long to") {
          it("int") {

            assertEncoderBehavior[JLong, Integer](
              SearchFieldDataType.INT64,
              DataTypes.IntegerType,
              2147483648L,
              _.intValue()
            )
          }

          it("long") {

            assertEncoderBehavior[JLong, JLong](
              SearchFieldDataType.INT64,
              DataTypes.LongType,
              2147483648L,
              identity
            )

            assertEncoderBehavior[Integer, JLong](
              SearchFieldDataType.INT64,
              DataTypes.LongType,
              123,
              _.longValue()
            )
          }

          it("double") {

            assertEncoderBehavior[JLong, JDouble](
              SearchFieldDataType.INT64,
              DataTypes.DoubleType,
              123,
              _.doubleValue()
            )
          }
        }

        describe("double to") {
          it("int") {

            assertEncoderBehavior[JDouble, Integer](
              SearchFieldDataType.DOUBLE,
              DataTypes.IntegerType,
              3.14,
              _.intValue()
            )
          }

          it("long") {

            assertEncoderBehavior[JDouble, Long](
              SearchFieldDataType.DOUBLE,
              DataTypes.LongType,
              123.456,
              _.longValue()
            )
          }

          it("double") {

            assertEncoderBehavior[JDouble, JDouble](
              SearchFieldDataType.DOUBLE,
              DataTypes.DoubleType,
              3.14,
              identity
            )
          }
        }
      }

      describe(s"throw a ${nameOf[MatchError]} for") {

        it("unsupported Search types") {

          a [MatchError] shouldBe thrownBy {

            assertEncoderBehavior[Integer, Integer](
              SearchFieldDataType.SINGLE,
              DataTypes.FloatType,
              1,
              identity
            )
          }
        }

        it("unsupported Spark types") {

          a [MatchError] shouldBe thrownBy {

            assertEncoderBehavior[Integer, Integer](
              SearchFieldDataType.INT32,
              DataTypes.FloatType,
              1,
              identity
            )
          }
        }

        it("Search values not matching Search type") {

          a [MatchError] shouldBe thrownBy {

            assertEncoderBehavior[Integer, Integer](
              SearchFieldDataType.DOUBLE,
              DataTypes.DoubleType,
              1,
              identity
            )
          }
        }
      }
    }
  }
}
