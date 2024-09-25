package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.SearchDocument
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.WriteConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructField

case class InternalRowToSearchDocumentConverter(private val converters: Map[StructField, WriteConverter])
  extends (InternalRow => SearchDocument) {

  override def apply(v1: InternalRow): SearchDocument = {

    val properties: Map[String, Object] = converters.zipWithIndex.map {
      case ((structField, converter), index) => (
        structField.name,
        converter.apply(v1.get(index, structField.dataType)
        )
      )
    }

    new SearchDocument(
      JavaScalaConverters.scalaMapToJava(
        properties
      )
    )
  }
}
