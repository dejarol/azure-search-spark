package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.SearchEncoder
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{CodecFactory, SearchIndexColumn, SchemaViolation}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Encoder for mapping a [[SearchDocument]] to an [[InternalRow]]
 * @param indexColumnToEncoders converters to apply in order to extract row values
 */

case class SearchDocumentToInternalRowConverter(private val indexColumnToEncoders: Map[SearchIndexColumn, SearchEncoder])
  extends (SearchDocument => InternalRow) {

  private lazy val sortedEncoders: Seq[(String, SearchEncoder)] = indexColumnToEncoders.toSeq.sortBy {
    case (k, _) => k.index()
  }.map {
    case (k, v) => (k.name(), v)
  }

  override def apply(v1: SearchDocument): InternalRow = {

    val values: Seq[Any] = sortedEncoders.map {
      case (name, encoder) => encoder.apply(v1.get(name))
    }

    InternalRow.fromSeq(values)
  }
}

object SearchDocumentToInternalRowConverter
  extends CodecFactory[SearchDocumentToInternalRowConverter, SearchEncoder] {

  override protected def getInternalMapping(
                                             schema: StructType,
                                             searchFields: Seq[SearchField]
                                           ): Either[Seq[SchemaViolation], Map[SearchIndexColumn, SearchEncoder]] = {

    EncodersSupplier.get(
      schema, searchFields
    )
  }

  override protected def toCodec(internal: Map[SearchIndexColumn, SearchEncoder]): SearchDocumentToInternalRowConverter = {

    SearchDocumentToInternalRowConverter(internal)
  }
}