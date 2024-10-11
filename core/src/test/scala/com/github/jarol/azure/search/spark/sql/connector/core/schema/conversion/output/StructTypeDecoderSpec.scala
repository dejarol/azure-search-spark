package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.FieldAdapterImpl
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataTypes

class StructTypeDecoderSpec
  extends BasicSpec {

  describe(anInstanceOf[StructTypeDecoder]) {
    describe(SHOULD) {
      it(s"convert an ${nameOf[InternalRow]} to a map") {

        val (k1, k2) = ("k1", "k2")
        val values = Seq(
          1,
          3.14
        )
        val input = InternalRow(values: _*)
        val output = StructTypeDecoder(
          Map(
            FieldAdapterImpl(k1, DataTypes.IntegerType) -> AtomicDecoders.Int32Converter,
            FieldAdapterImpl(k2, DataTypes.DoubleType) -> AtomicDecoders.DoubleConverter
          )
        ).apply(input)

        output should contain key k1
        output.get(k1) shouldBe values.head
        output should contain key k2
        output.get(k2) shouldBe values(1)
      }
    }
  }
}
