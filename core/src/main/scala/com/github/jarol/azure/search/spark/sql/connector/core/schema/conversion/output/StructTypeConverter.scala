package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.FieldAdapter
import org.apache.spark.sql.catalyst.InternalRow

import java.util

/**
 * Converter for Spark internal rows
 * @param converters converters to apply on internal row subfields
 */

case class StructTypeConverter(private val converters: Map[FieldAdapter, WriteConverter])
  extends WriteTransformConverter[util.Map[String,  Object]] {

  override protected def transform(value: Any): util.Map[String, Object] = {

    val internalRow = value.asInstanceOf[InternalRow]
    val scalaMap: Map[String, Object] = converters.zipWithIndex.map {
      case ((field, converter), index) => (
        field.name,
        converter.apply(
          internalRow.get(index, field.sparkType())
        )
      )
    }

    JavaScalaConverters.scalaMapToJava(scalaMap)
  }
}
