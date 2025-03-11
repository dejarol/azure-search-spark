package io.github.jarol.azure.search.spark.connector.core.schema.conversion.output

import io.github.jarol.azure.search.spark.connector.core.BasicSpec
import io.github.jarol.azure.search.spark.connector.core.schema.conversion.SearchIndexColumnImpl
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
            SearchIndexColumnImpl(k1, DataTypes.IntegerType, 0) -> AtomicDecoders.identity(),
            SearchIndexColumnImpl(k2, DataTypes.DoubleType, 1) -> AtomicDecoders.identity()
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
