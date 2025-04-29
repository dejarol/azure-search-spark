package io.github.dejarol.azure.search.spark.connector.core.schema.conversion.input

import io.github.dejarol.azure.search.spark.connector.BasicSpec
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.read.encoding.{AtomicEncoders, CollectionEncoder}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

class CollectionEncoderSpec
  extends BasicSpec {

  describe(anInstanceOf[CollectionEncoder]) {
    describe(SHOULD) {
      it(s"convert a list into an ${nameOf[ArrayData]}") {

        val input = Seq("hello", "world")
        val output = CollectionEncoder(
          AtomicEncoders.forUTF8Strings()
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
