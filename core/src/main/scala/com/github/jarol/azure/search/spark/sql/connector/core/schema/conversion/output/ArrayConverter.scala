package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

import java.util

/**
 * Converter for Spark arrays
 * @param arrayInternalConverter converter for array internal objects
 */

case class ArrayConverter(private val arrayInternalType: DataType,
                          private val arrayInternalConverter: WriteConverter)
  extends WriteTransformConverter[util.List[Object]] {

  override protected def transform(value: Any): util.List[Object] = {

    JavaScalaConverters.seqToList(
      value.asInstanceOf[ArrayData]
        .toSeq(arrayInternalType)
        .map(arrayInternalConverter.toSearchProperty)
    )
  }
}
