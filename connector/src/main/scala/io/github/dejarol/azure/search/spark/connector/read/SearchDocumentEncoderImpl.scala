package io.github.dejarol.azure.search.spark.connector.read

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import io.github.dejarol.azure.search.spark.connector.core.schema.{CodecCreationException, CodecType}
import io.github.dejarol.azure.search.spark.connector.core.schema.conversion.input.ComplexEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * A concrete encoder implementation
 * @param delegate encoders to apply in order to extract row values
 */

case class SearchDocumentEncoderImpl(private val delegate: ComplexEncoder)
  extends SearchDocumentEncoder {

  override def apply(document: SearchDocument): InternalRow = delegate.apply(document)
}

object SearchDocumentEncoderImpl {

  /**
   * Safely create a document encoder instance
   *
   * @param schema target Dataframe schema
   * @param searchFields input Search fields
   * @return either the encoder or a
   *         [[CodecCreationException]]
   */

  final def safeApply(
                       schema: StructType,
                       searchFields: Seq[SearchField]
                     ): Either[CodecCreationException, SearchDocumentEncoderImpl] = {

    EncoderFactory.buildComplexCodecInternalMapping(
      schema, searchFields
    ).left.map {
      error => CodecCreationException.fromCodedError(
        CodecType.ENCODING, error
      )
    }.right.map(
      value => SearchDocumentEncoderImpl(
        ComplexEncoder(value)
      )
    )
  }
}