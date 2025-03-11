package io.github.jarol.azure.search.spark.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.SearchField
import io.github.jarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.jarol.azure.search.spark.connector.core.schema.conversion.output.SearchDecoder
import io.github.jarol.azure.search.spark.connector.core.schema.conversion.{SchemaViolationException, SearchIndexColumn}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import java.util.{LinkedHashMap => JLinkedMap}

/**
 * Decoder for translating an [[InternalRow]] to a [[SearchDocument]]
 * @param indexColumnToSearchDecoders converters for retrieving document values from an internal row
 */

case class SearchDocumentDecoder(private val indexColumnToSearchDecoders: Map[SearchIndexColumn, SearchDecoder])
  extends (InternalRow => SearchDocument) {

  // Sorted collection of document properties
  private lazy val sortedColumns: Seq[(SearchIndexColumn, SearchDecoder)] = indexColumnToSearchDecoders
    .toSeq.sortBy {
      case (column, _) => column.index()
  }

  override def apply(v1: InternalRow): SearchDocument = {

    // Create a map with non-null properties
    val properties: JLinkedMap[String, Object] = new JLinkedMap()

    // Exclude null properties
    sortedColumns.filterNot {
      case (k, _) => v1.isNullAt(k.index())
    }.foreach {
      case (k, v) =>

        // Add property
        properties.put(
          k.name(),
          v.apply(v1.get(k.index(), k.sparkType()))
      )
    }

    // Create a new document from this properties
    new SearchDocument(properties)
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
