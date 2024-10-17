package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.SearchDecoder
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{CodecFactory, SearchIndexColumn, SchemaViolation}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Converter for mapping an [[InternalRow]] to a [[SearchDocument]]
 * @param indexColumnToSearchDecoders converters for retrieving document values from an internal row
 */

case class InternalRowToSearchDocumentConverter(private val indexColumnToSearchDecoders: Map[SearchIndexColumn, SearchDecoder])
  extends (InternalRow => SearchDocument) {

  override def apply(v1: InternalRow): SearchDocument = {

    // Create a map with non-null properties
    val properties: Map[String, Object] = indexColumnToSearchDecoders.collect {
      case (field, decoder) if !v1.isNullAt(field.index()) =>
        (
          field.name,
          decoder.apply(v1.get(field.index(), field.sparkType()))
        )
    }

    // Create a new document from this properties
    new SearchDocument(
      JavaScalaConverters.scalaMapToJava(
        properties
      )
    )
  }
}

object InternalRowToSearchDocumentConverter
  extends CodecFactory[InternalRowToSearchDocumentConverter, SearchDecoder] {

  override protected def getInternalMapping(
                                             schema: StructType,
                                             searchFields: Seq[SearchField]
                                           ): Either[Seq[SchemaViolation], Map[SearchIndexColumn, SearchDecoder]] = {

    DecodersSupplier.get(
      schema, searchFields
    )
  }

  override protected def toCodec(internal: Map[SearchIndexColumn, SearchDecoder]): InternalRowToSearchDocumentConverter = {

    InternalRowToSearchDocumentConverter(internal)
  }
}
