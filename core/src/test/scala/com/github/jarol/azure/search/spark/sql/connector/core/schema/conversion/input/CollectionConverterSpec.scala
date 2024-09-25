package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, JavaScalaConverters}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

class CollectionConverterSpec
  extends BasicSpec {

  describe(anInstanceOf[CollectionConverter]) {
    describe(SHOULD) {
      it(s"convert a list into an ${nameOf[ArrayData]}") {

        val input = Seq("hello", "world")
        val output = CollectionConverter(
          AtomicReadConverters.StringConverter
        ).apply(
          JavaScalaConverters.seqToList(input)
        )

        val actual: Seq[UTF8String] = output
          .toSeq[UTF8String](DataTypes.StringType)

        val expected = input.map(UTF8String.fromString)
        actual should contain theSameElementsAs expected
      }
    }
  }
}
