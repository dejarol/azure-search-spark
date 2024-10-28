package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.SearchIndexColumnImpl
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataTypes

import java.util.{LinkedHashMap => JLinkedMap, Map => JMap}

class ComplexEncoderSpec
  extends BasicSpec {

  private lazy val (k1, k2, v1, v2) = ("k1", "k2", "hello", 1)
  private lazy val input: JMap[String, Object] = new JLinkedMap() {{
    put(k1, v1)
    put(k2, Integer.valueOf(v2))
  }}

  describe(anInstanceOf[ComplexEncoder]) {
    describe(SHOULD) {
      describe(s"convert a map into an ${nameOf[InternalRow]}") {
        describe("when schema order") {
          it("matches with search properties order") {

            val output = ComplexEncoder(
              Map(
                SearchIndexColumnImpl(k1, DataTypes.StringType, 0) -> AtomicEncoders.forUTF8Strings(),
                SearchIndexColumnImpl(k2, DataTypes.IntegerType, 1) -> AtomicEncoders.identity()
              )
            ).apply(input)

            output.getString(0) shouldBe v1
            output.getInt(1) shouldBe v2
          }

          it("does not match with search properties order") {

            val output = ComplexEncoder(
              Map(
                SearchIndexColumnImpl(k2, DataTypes.IntegerType, 0) -> AtomicEncoders.identity(),
                SearchIndexColumnImpl(k1, DataTypes.StringType, 1) -> AtomicEncoders.forUTF8Strings()
              )
            ).apply(input)

            output.getInt(0) shouldBe v2
            output.getString(1) shouldBe v1
          }
        }
      }
    }
  }
}
