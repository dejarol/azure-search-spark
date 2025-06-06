package io.github.dejarol.azure.search.spark.connector.write.decoding

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

import java.util.{List => JList}

/**
 * Decoder for Spark arrays
 * @param internalDecoder converter for array internal objects
 */

case class ArrayDecoder(private val internalType: DataType,
                        private val internalDecoder: SearchDecoder)
  extends TransformDecoder[JList[Object]] {

  override protected def transform(value: Any): JList[Object] = {

    JavaScalaConverters.seqToList(
      value.asInstanceOf[ArrayData]
        .toSeq(internalType)
        .map(internalDecoder.apply)
    )
  }
}
