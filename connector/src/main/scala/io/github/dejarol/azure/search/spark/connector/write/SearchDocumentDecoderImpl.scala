package io.github.dejarol.azure.search.spark.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import io.github.dejarol.azure.search.spark.connector.core.schema.{CodecFactoryException, CodecType}
import io.github.dejarol.azure.search.spark.connector.core.schema.conversion.output.StructTypeDecoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

/**
 * A concrete decoder implementation
 * @param delegate delegate object for internal row decoding
 */

case class SearchDocumentDecoderImpl(private val delegate: StructTypeDecoder)
  extends SearchDocumentDecoder {

  override def apply(row: InternalRow): SearchDocument = {

    // Create a map with non-null properties
    val properties: JMap[String, Object] = delegate.apply(row)
    new SearchDocument(properties)
  }
}

object SearchDocumentDecoderImpl {

  /**
   * Safely create a decoder
   * @param schema Dataframe schema
   * @param searchFields target Search fields
   * @return either the decoder or a
   *         [[io.github.dejarol.azure.search.spark.connector.core.schema.CodecFactoryException]]
   */

  final def safeApply(
                       schema: StructType,
                       searchFields: Seq[SearchField]
                     ): Either[CodecFactoryException, SearchDocumentDecoderImpl] = {

    DecoderFactory.buildComplexCodecInternalMapping(
      schema,
      searchFields
    ).left.map {
      // TODO : fix
      _ => CodecFactoryException.forGeoPoint("a", CodecType.DECODING)
      }
      .right.map {
      // Create decoder
      v => SearchDocumentDecoderImpl(
        StructTypeDecoder(v)
      )
    }
  }
}
