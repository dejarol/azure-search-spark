package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.SearchEncoder
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{CodecFactory, FieldAdapter, SchemaViolation}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructField

/**
 * Converter for mapping a [[SearchDocument]] to an [[InternalRow]]
 * @param converters converters to apply in order to extract row values
 */

case class SearchDocumentToInternalRowConverter(private val converters: Map[FieldAdapter, SearchEncoder])
  extends (SearchDocument => InternalRow) {

  override def apply(v1: SearchDocument): InternalRow = {

    val values: Seq[Any] = converters.map {
      case (field, converter) => converter.apply(v1.get(field.name))
    }.toSeq

    InternalRow.fromSeq(values)
  }
}

object SearchDocumentToInternalRowConverter
  extends CodecFactory[SearchDocumentToInternalRowConverter, SearchEncoder] {

  override protected def getInternalMapping(
                                             schema: Seq[StructField],
                                             searchFields: Seq[SearchField]
                                           ): Either[Seq[SchemaViolation], Map[FieldAdapter, SearchEncoder]] = {

    EncodingSupplier.get(
      schema, searchFields
    )
  }

  override protected def toConverter(internal: Map[FieldAdapter, SearchEncoder]): SearchDocumentToInternalRowConverter = {

    SearchDocumentToInternalRowConverter(internal)
  }
}