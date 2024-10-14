package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

import java.util

/**
 * Decoder for Spark arrays
 * @param internalDecoder converter for array internal objects
 */

case class ArrayDecoder(private val internalType: DataType,
                        private val internalDecoder: SearchDecoder)
  extends TransformDecoder[util.List[Object]] {

  override protected def transform(value: Any): util.List[Object] = {

    JavaScalaConverters.seqToList(
      value.asInstanceOf[ArrayData]
        .toSeq(internalType)
        .map(internalDecoder.apply)
    )
  }
}
