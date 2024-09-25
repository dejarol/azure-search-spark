package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, JavaScalaConverters}
import org.apache.spark.sql.catalyst.InternalRow

import java.util

class ComplexConverterSpec
  extends BasicSpec {

  describe(anInstanceOf[ComplexConverter]) {
    describe(SHOULD) {
      it(s"convert a map into an ${nameOf[InternalRow]}") {

        val (k1, k2, v1, v2) = ("k1", "k2", "hello", 1)
        val input: util.Map[String, Object] = JavaScalaConverters.scalaMapToJava(
          Map(
            k1 -> v1,
            k2 -> java.lang.Integer.valueOf(v2)
          )
        )

        val output = ComplexConverter(
          Map(
            k1 -> AtomicReadConverters.StringConverter,
            k2 -> AtomicReadConverters.Int32Converter
          )
        ).apply(
          input
        )

        output.getString(0) shouldBe v1
        output.getInt(1) shouldBe v2
      }
    }
  }
}
