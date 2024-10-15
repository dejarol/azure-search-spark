package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.FieldAdapterImpl
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, JavaScalaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataTypes

import java.util.{Map => JMap}

class ComplexEncoderSpec
  extends BasicSpec {

  describe(anInstanceOf[ComplexEncoder]) {
    describe(SHOULD) {
      it(s"convert a map into an ${nameOf[InternalRow]}") {

        val (k1, k2, v1, v2) = ("k1", "k2", "hello", 1)
        val input: JMap[String, Object] = JavaScalaConverters.scalaMapToJava(
          Map(
            k1 -> v1,
            k2 -> Integer.valueOf(v2)
          )
        )

        val output = ComplexEncoder(
          Map(
            FieldAdapterImpl(k1, DataTypes.StringType) -> AtomicEncoders.forUTF8Strings(),
            FieldAdapterImpl(k2, DataTypes.IntegerType) -> AtomicEncoders.identity()
          )
        ).apply(input)

        output.getString(0) shouldBe v1
        output.getInt(1) shouldBe v2
      }
    }
  }
}
