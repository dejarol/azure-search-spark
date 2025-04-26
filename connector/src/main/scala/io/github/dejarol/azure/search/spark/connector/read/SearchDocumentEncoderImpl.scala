package io.github.dejarol.azure.search.spark.connector.read

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import io.github.dejarol.azure.search.spark.connector.core.schema.CodecFactoryException
import io.github.dejarol.azure.search.spark.connector.core.schema.conversion.SearchIndexColumn
import io.github.dejarol.azure.search.spark.connector.core.schema.conversion.input.{ComplexEncoder, SearchEncoder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * A concrete encoder implementation
 * @param delegateInternalMapping encoders to apply in order to extract row values
 */

case class SearchDocumentEncoderImpl(private val delegateInternalMapping: Map[SearchIndexColumn, SearchEncoder])
  extends SearchDocumentEncoder {

  private lazy val delegate = ComplexEncoder(delegateInternalMapping)

  override def apply(document: SearchDocument): InternalRow = delegate.apply(document)
}

object SearchDocumentEncoderImpl {

  /**
   * Safely create a document encoder instance
   * @param schema target Dataframe schema
   * @param searchFields input Search fields
   * @return either the encoder or a
   *         [[io.github.dejarol.azure.search.spark.connector.core.schema.CodecFactoryException]]
   */

  final def safeApply(
                       schema: StructType,
                       searchFields: Seq[SearchField]
                     ): Either[CodecFactoryException, SearchDocumentEncoderImpl] = {

    EncodersFactory.buildComplexCodecInternalMapping(
      schema, searchFields,
    ).right.map(
      value => SearchDocumentEncoderImpl(value)
    )
  }
}