package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.WriteConverter
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{ConverterFactory, FieldAdapter, SchemaViolation}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructField

/**
 * Converter for mapping an [[InternalRow]] to a [[SearchDocument]]
 * @param converters converters for retrieving document values from an internal row
 */

case class InternalRowToSearchDocumentConverter(private val converters: Map[FieldAdapter, WriteConverter])
  extends (InternalRow => SearchDocument) {

  override def apply(v1: InternalRow): SearchDocument = {

    // Create a map with non-null properties
    val properties: Map[String, Object] = converters.zipWithIndex.collect {
      case ((field, converter), index) if !v1.isNullAt(index) =>
        (
          field.name,
          converter.apply(v1.get(index, field.sparkType()))
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
  extends ConverterFactory[InternalRowToSearchDocumentConverter, WriteConverter] {

  override protected def getInternalMapping(
                                             schema: Seq[StructField],
                                             searchFields: Seq[SearchField]
                                           ): Either[Seq[SchemaViolation], Map[FieldAdapter, WriteConverter]] = {

    WriteMappingSupplier.safelyGet(
      schema, searchFields
    )
  }

  override protected def toConverter(internal: Map[FieldAdapter, WriteConverter]): InternalRowToSearchDocumentConverter = {

    InternalRowToSearchDocumentConverter(internal)
  }
}
