package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.SearchDecoder
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{CodecFactory, FieldAdapter, SchemaViolation}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Converter for mapping an [[InternalRow]] to a [[SearchDocument]]
 * @param converters converters for retrieving document values from an internal row
 */

case class InternalRowToSearchDocumentConverter(private val converters: Map[FieldAdapter, SearchDecoder])
  extends (InternalRow => SearchDocument) {

  override def apply(v1: InternalRow): SearchDocument = {

    // Create a map with non-null properties
    val properties: Map[String, Object] = converters.collect {
      case (field, converter) if !v1.isNullAt(field.index()) =>
        (
          field.name,
          converter.apply(v1.get(field.index(), field.sparkType()))
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
                                           ): Either[Seq[SchemaViolation], Map[FieldAdapter, SearchDecoder]] = {

    DecodersSupplier.get(
      schema, searchFields
    )
  }

  override protected def toConverter(internal: Map[FieldAdapter, SearchDecoder]): InternalRowToSearchDocumentConverter = {

    InternalRowToSearchDocumentConverter(internal)
  }
}
