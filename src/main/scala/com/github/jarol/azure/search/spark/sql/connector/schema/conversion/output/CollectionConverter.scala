package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import org.apache.spark.sql.catalyst.util.ArrayData

import java.util

case class CollectionConverter(private val arrayInternalConverter: SearchPropertyConverter)
  extends SearchPropertyTransformConverter[util.List[Object]] {

  override protected def transform(value: Any): util.List[Object] = {

    JavaScalaConverters.seqToList(
      value.asInstanceOf[ArrayData].array.map {
        arrayInternalConverter.toSearchProperty
      }
    )
  }
}
