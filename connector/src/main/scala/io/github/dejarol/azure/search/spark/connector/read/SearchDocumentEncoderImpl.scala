package io.github.dejarol.azure.search.spark.connector.read

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.schema.conversion.{SchemaViolationException, SearchIndexColumn}
import io.github.dejarol.azure.search.spark.connector.core.schema.conversion.input.SearchEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * A concrete encoder implementation
 * @param indexColumnToEncoders encoders to apply in order to extract row values
 */

case class SearchDocumentEncoderImpl(private val indexColumnToEncoders: Map[SearchIndexColumn, SearchEncoder])
  extends SearchDocumentEncoder {

  private lazy val sortedEncoders: Seq[(String, SearchEncoder)] = indexColumnToEncoders.toSeq.sortBy {
    case (k, _) => k.index()
  }.map {
    case (k, v) => (k.name(), v)
  }

  override def apply(document: SearchDocument): InternalRow = {

    val values: Seq[Any] = sortedEncoders.map {
      case (name, encoder) => encoder.apply(document.get(name))
    }

    InternalRow.fromSeq(values)
  }
}

object SearchDocumentEncoderImpl {

  /**
   * Safely create a document encoder instance
   * @param schema target Dataframe schema
   * @param searchFields input Search fields
   * @return either the encoder or a
   *         [[io.github.dejarol.azure.search.spark.connector.core.schema.conversion.SchemaViolationException]]
   */

  final def safeApply(
                       schema: StructType,
                       searchFields: Seq[SearchField]
                     ): Either[SchemaViolationException, SearchDocumentEncoderImpl] = {

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
      v => SearchDocumentEncoderImpl(v)
    }
  }
}