package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

class ArrayDecoderSpec
  extends BasicSpec {

  describe(anInstanceOf[ArrayDecoder]) {
    describe(SHOULD) {
      it("convert an array to a list") {

        val input = Seq("hello", "world")
        val output = ArrayDecoder(
          DataTypes.StringType,
          AtomicDecoders.StringConverter
        ).apply(
          ArrayData.toArrayData(
            input.map(UTF8String.fromString)
          )
        )

        output should have size input.size
        output should contain theSameElementsAs input
      }
    }
  }
}
