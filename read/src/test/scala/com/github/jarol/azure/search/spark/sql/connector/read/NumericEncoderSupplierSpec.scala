package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.TransformEncoder

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong}

class NumericEncoderSupplierSpec
  extends BasicSpec {

  /**
   * Assert that an encoder exists for a given Search type, and that its behavior reflects an expected function
   * @param supplier instance of [[NumericEncoderSupplier]]
   * @param searchType input Search type (for which we would test the encoder existence)
   * @param input test input
   * @param expectedFunction expected function to be applied by the existing encoder
   * @tparam SearchT Search input data type in Java/Scala
   * @tparam SparkT Spark output data type in Java/Scala
   */

  private def assertEncoderFor[SearchT, SparkT](
                                                 supplier: NumericEncoderSupplier,
                                                 searchType: SearchFieldDataType,
                                                 input: SearchT,
                                                 expectedFunction: SearchT => SparkT
                                               ): Unit = {

    val maybeEncoder = supplier.getForType(searchType)
    maybeEncoder shouldBe defined
    val encoder = maybeEncoder.get
    encoder shouldBe a[TransformEncoder[_]]
    encoder.apply(input) shouldBe expectedFunction(input)
  }

  describe(`object`[NumericEncoderSupplier]) {
    describe(SHOULD) {
      describe("define encoders for handling numeric data, as") {
        it("int") {

          val supplier = NumericEncoderSupplier.INT_32
          assertEncoderFor[Integer, Integer](supplier, SearchFieldDataType.INT32, 1, identity)
          assertEncoderFor[JLong, Integer](supplier, SearchFieldDataType.INT64, 123, _.intValue())
          assertEncoderFor[JDouble, Integer](supplier, SearchFieldDataType.DOUBLE, 3.14, _.intValue())
          assertEncoderFor[JFloat, Integer](supplier, SearchFieldDataType.SINGLE, 3.14f, _.intValue())
        }

        it("long") {

          val supplier = NumericEncoderSupplier.INT_64
          assertEncoderFor[Integer, JLong](supplier, SearchFieldDataType.INT32, 1, _.longValue())
          assertEncoderFor[JLong, JLong](supplier, SearchFieldDataType.INT64, 123, identity)
          assertEncoderFor[JDouble, JLong](supplier, SearchFieldDataType.DOUBLE, 3.14, _.longValue())
          assertEncoderFor[JFloat, JLong](supplier, SearchFieldDataType.SINGLE, 3.14f, _.longValue())
        }

        it("double") {

          val supplier = NumericEncoderSupplier.DOUBLE
          assertEncoderFor[Integer, JDouble](supplier, SearchFieldDataType.INT32, 1, _.doubleValue())
          assertEncoderFor[JLong, JDouble](supplier, SearchFieldDataType.INT64, 123, _.doubleValue())
          assertEncoderFor[JDouble, JDouble](supplier, SearchFieldDataType.DOUBLE, 3.14, identity)
          assertEncoderFor[JFloat, JDouble](supplier, SearchFieldDataType.SINGLE, 3.14f, _.doubleValue())
        }

        it("single") {

          val supplier = NumericEncoderSupplier.SINGLE
          assertEncoderFor[Integer, JFloat](supplier, SearchFieldDataType.INT32, 1, _.floatValue())
          assertEncoderFor[JLong, JFloat](supplier, SearchFieldDataType.INT64, 123, _.floatValue())
          assertEncoderFor[JDouble, JFloat](supplier, SearchFieldDataType.DOUBLE, 3.14, _.floatValue())
          assertEncoderFor[JFloat, JFloat](supplier, SearchFieldDataType.SINGLE, 3.14f, identity)
        }
      }
    }
  }
}
