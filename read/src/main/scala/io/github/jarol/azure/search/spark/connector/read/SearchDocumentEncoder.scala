package io.github.jarol.azure.search.spark.connector.read

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import io.github.jarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.jarol.azure.search.spark.connector.core.schema.conversion.input.SearchEncoder
import io.github.jarol.azure.search.spark.connector.core.schema.conversion.{SchemaViolationException, SearchIndexColumn}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Encoder for translating a [[SearchDocument]] into an [[InternalRow]]
 * @param indexColumnToEncoders encoders to apply in order to extract row values
 */

case class SearchDocumentEncoder(private val indexColumnToEncoders: Map[SearchIndexColumn, SearchEncoder])
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

object SearchDocumentEncoder {

  /**
   * Safely create a document encoder instance
   * @param schema target Dataframe schema
   * @param searchFields input Search fields
   * @return either the encoder or a [[SchemaViolationException]]
   */

  final def safeApply(
                       schema: StructType,
                       searchFields: Seq[SearchField]
                     ): Either[SchemaViolationException, SearchDocumentEncoder] = {

    EncodersSupplier.get(
      schema,
      searchFields
    ).left.map {
      // Map left side
      v => new SchemaViolationException(
        JavaScalaConverters.seqToList(v)
      )
    }.right.map {
      // Create encoder
      v => SearchDocumentEncoder(v)
    }
  }
}