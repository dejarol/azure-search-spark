package com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output

import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructField

import java.util

case class StructTypeConverter(private val converters: Map[StructField, SearchPropertyConverter])
  extends SearchPropertyTransformConverter[util.Map[String,  Object]] {

  override protected def transform(value: Any): util.Map[String, Object] = {

    val internalRow = value.asInstanceOf[InternalRow]
    val scalaMap: Map[String, Object] = converters.zipWithIndex.map {
      case ((structField, converter), index) => (
        structField.name,
        converter.toSearchProperty(
          internalRow.get(index, structField.dataType)
        )
      )
    }

    JavaScalaConverters.scalaMapToJava(scalaMap)
  }
}
