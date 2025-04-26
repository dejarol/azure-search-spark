package io.github.dejarol.azure.search.spark.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.schema.conversion.{SchemaViolationException, SearchIndexColumn}
import io.github.dejarol.azure.search.spark.connector.core.schema.conversion.output.{SearchDecoder, StructTypeDecoder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

/**
 * A concrete decoder implementation
 * @param delegateInternalMapping converters for retrieving document values from an internal row
 */

case class SearchDocumentDecoderImpl(private val delegateInternalMapping: Map[SearchIndexColumn, SearchDecoder])
  extends SearchDocumentDecoder {

  // Sorted collection of document properties
  private lazy val delegate = StructTypeDecoder(delegateInternalMapping)

  override def apply(row: InternalRow): SearchDocument = {

    // Create a map with non-null properties
    val properties: JMap[String, Object] = delegate.apply(row)
    new SearchDocument(properties)
  }
}

object SearchDocumentDecoderImpl {

  /**
   * Safely create a document decoder instance
   * @param schema Dataframe schema
   * @param searchFields target Search fields
   * @return either the decoder or a
   *         [[io.github.dejarol.azure.search.spark.connector.core.schema.conversion.SchemaViolationException]]
   */

  final def safeApply(
                       schema: StructType,
                       searchFields: Seq[SearchField]
                     ): Either[SchemaViolationException, SearchDocumentDecoderImpl] = {

    DecoderFactory.get(
      schema,
      searchFields
    ).left.map {
      // Map left side
      v => new SchemaViolationException(
        JavaScalaConverters.seqToList(v)
      )
    }.right.map {
      // Create decoder
      v => SearchDocumentDecoderImpl(v)
    }
  }
}
