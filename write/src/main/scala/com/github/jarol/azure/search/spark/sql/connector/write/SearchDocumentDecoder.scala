package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.SearchDecoder
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{SchemaViolationException, SearchIndexColumn}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Decoder for translating an [[InternalRow]] to a [[SearchDocument]]
 * @param indexColumnToSearchDecoders converters for retrieving document values from an internal row
 */

case class SearchDocumentDecoder(private val indexColumnToSearchDecoders: Map[SearchIndexColumn, SearchDecoder])
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

object SearchDocumentDecoder {

  /**
   * Safely create a document decoder instance
   * @param schema Dataframe schema
   * @param searchFields target Search fields
   * @return either the decoder or a [[SchemaViolationException]]
   */

  final def safeApply(
                       schema: StructType,
                       searchFields: Seq[SearchField]
                     ): Either[SchemaViolationException, SearchDocumentDecoder] = {

    DecodersSupplier.get(
      schema,
      searchFields
    ).left.map {
      // Map left side
      v => new SchemaViolationException(
        JavaScalaConverters.seqToList(v)
      )
    }.right.map {
      // Create decoder
      v => SearchDocumentDecoder(v)
    }
  }
}
