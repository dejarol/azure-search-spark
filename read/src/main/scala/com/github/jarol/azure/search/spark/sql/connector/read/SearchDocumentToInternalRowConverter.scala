package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.SearchDocument
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.FieldAdapter
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.ReadConverter
import org.apache.spark.sql.catalyst.InternalRow

/**
 * Converter for mapping a [[SearchDocument]] to an [[InternalRow]]
 * @param converters converters to apply in order to extract row values
 */

case class SearchDocumentToInternalRowConverter(private val converters: Map[FieldAdapter, ReadConverter])
  extends (SearchDocument => InternalRow) {

  override def apply(v1: SearchDocument): InternalRow = {

    val values: Seq[Any] = converters.map {
      case (field, converter) => converter.apply(v1.get(field.name))
    }.toSeq

    InternalRow.fromSeq(values)
  }
}