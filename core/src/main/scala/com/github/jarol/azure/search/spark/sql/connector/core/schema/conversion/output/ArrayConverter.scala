package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import org.apache.spark.sql.catalyst.util.ArrayData

import java.util

/**
 * Converter for Spark arrays
 * @param arrayInternalConverter converter for array internal objects
 */

case class ArrayConverter(private val arrayInternalConverter: SearchPropertyConverter)
  extends SearchPropertyTransformConverter[util.List[Object]] {

  override protected def transform(value: Any): util.List[Object] = {

    JavaScalaConverters.seqToList(
      value.asInstanceOf[ArrayData].array.map {
        arrayInternalConverter.toSearchProperty
      }
    )
  }
}
