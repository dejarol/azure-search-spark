package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}

class NumericDecoderSupplierSpec
  extends BasicSpec {

  /**
   * Assert that a decoder exists and that it resembles the behavior of an expected function
   * @param supplier instance of [[NumericDecoderSupplier]]
   * @param searchFieldDataType target Search field data type
   * @param input input value
   * @param expectedFunction expected decoding function
   * @tparam SparkType Spark internal Java/Scala type
   * @tparam SearchType Search Java/Scala type
   */

  private def assertDecoderFor[SparkType, SearchType](
                                                       supplier: NumericDecoderSupplier,
                                                       searchFieldDataType: SearchFieldDataType,
                                                       input: SparkType,
                                                       expectedFunction: SparkType => SearchType
                                                     ): Unit = {

    val maybeDecoder = supplier.getForType(searchFieldDataType)
    maybeDecoder shouldBe defined
    val decoder = maybeDecoder.get
    decoder.apply(input) shouldBe expectedFunction(input)
  }

  describe(`object`[NumericDecoderSupplier]) {
    describe(SHOULD) {
      describe("define decoder for handling numeric data, like") {
        it("int") {

          val supplier = NumericDecoderSupplier.INT32
          assertDecoderFor[Integer, Integer](supplier, SearchFieldDataType.INT32, 1, identity)
          assertDecoderFor[Integer, JLong](supplier, SearchFieldDataType.INT64, 123, _.longValue())
          assertDecoderFor[Integer, JDouble](supplier, SearchFieldDataType.DOUBLE, 3, _.doubleValue())
          assertDecoderFor[Integer, JFloat](supplier, SearchFieldDataType.SINGLE, 4, _.floatValue())
        }

        it("long") {

          val supplier = NumericDecoderSupplier.INT64
          assertDecoderFor[JLong, Integer](supplier, SearchFieldDataType.INT32, 123, _.intValue())
          assertDecoderFor[JLong, JLong](supplier, SearchFieldDataType.INT64, 123, identity)
          assertDecoderFor[JLong, JDouble](supplier, SearchFieldDataType.DOUBLE, 345, _.doubleValue())
          assertDecoderFor[JLong, JFloat](supplier, SearchFieldDataType.SINGLE, 456, _.floatValue())
        }

        it("double") {

          val supplier = NumericDecoderSupplier.DOUBLE
          assertDecoderFor[JDouble, Integer](supplier, SearchFieldDataType.INT32, 123, _.intValue())
          assertDecoderFor[JDouble, JLong](supplier, SearchFieldDataType.INT64, 123, _.longValue())
          assertDecoderFor[JDouble, JDouble](supplier, SearchFieldDataType.DOUBLE, 345, identity)
          assertDecoderFor[JDouble, JFloat](supplier, SearchFieldDataType.SINGLE, 456, _.floatValue())
        }

        it("float") {

          val supplier = NumericDecoderSupplier.SINGLE
          assertDecoderFor[JFloat, Integer](supplier, SearchFieldDataType.INT32, 123, _.intValue())
          assertDecoderFor[JFloat, JLong](supplier, SearchFieldDataType.INT64, 123, _.longValue())
          assertDecoderFor[JFloat, JDouble](supplier, SearchFieldDataType.DOUBLE, 345, _.doubleValue())
          assertDecoderFor[JFloat, JFloat](supplier, SearchFieldDataType.SINGLE, 456, identity)
        }
      }
    }
  }
}
