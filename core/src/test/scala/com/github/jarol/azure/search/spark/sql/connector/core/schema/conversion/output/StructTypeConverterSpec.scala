package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataTypes

class StructTypeConverterSpec
  extends BasicSpec
    with FieldFactory {

  describe(anInstanceOf[StructTypeConverter]) {
    describe(SHOULD) {
      it(s"convert an ${nameOf[InternalRow]} to a map") {

        val (k1, k2) = ("k1", "k2")
        val values = Seq(
          1,
          3.14
        )
        val input = InternalRow(values: _*)
        val output = StructTypeConverter(
          Map(
            createStructField(k1, DataTypes.IntegerType) -> AtomicWriteConverters.Int32Converter,
            createStructField(k2, DataTypes.DoubleType) -> AtomicWriteConverters.DoubleConverter
          )
        ).toSearchProperty(input)

        output should contain key k1
        output.get(k1) shouldBe values.head
        output should contain key k2
        output.get(k2) shouldBe values(1)
      }
    }
  }
}
