package io.github.dejarol.azure.search.spark.connector.read

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import io.github.dejarol.azure.search.spark.connector.core.schema.{CodecFactoryException, CodecType}
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
      schema, searchFields
    ).left.map {
      _ => CodecFactoryException.forGeoPoint("a", CodecType.ENCODING)
      }.right.map(
      value => SearchDocumentEncoderImpl(
        ComplexEncoder(value)
      )
    )
  }
}