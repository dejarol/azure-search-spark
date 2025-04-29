package io.github.dejarol.azure.search.spark.connector.core.schema.conversion.output

import io.github.dejarol.azure.search.spark.connector.core.codec.{SearchIndexColumn, SearchIndexColumnImpl}
import io.github.dejarol.azure.search.spark.connector.core.schema.GeoPointType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructType}

import java.util.{LinkedHashMap => JLinkedMap, Map => JMap}

/**
 * Decoder for Spark internal rows
 * @param indexColumnToSearchDecoders decoders to apply on internal row subfields
 */

case class StructTypeDecoder(private val indexColumnToSearchDecoders: Map[SearchIndexColumn, SearchDecoder])
  extends TransformDecoder[JMap[String,  Object]] {

  // Sorted collection of document properties
  private lazy val sortedColumns: Seq[(SearchIndexColumn, SearchDecoder)] = indexColumnToSearchDecoders
    .toSeq.sortBy {
      case (column, _) => column.index()
  }

  override protected def transform(value: Any): JMap[String, Object] = {

    val row = value.asInstanceOf[InternalRow]
    val properties = new JLinkedMap[String, Object]()

    // Exclude null properties
    sortedColumns.filterNot {
      case (k, _) => row.isNullAt(k.index())
    }.foreach {
      case (k, v) =>

        // Add property
        properties.put(
          k.name,
          v.apply(row.get(k.index(), k.sparkType()))
      )
    }

    properties
  }
}

object StructTypeDecoder {

  /**
   * Create a decoder for deal with GeoPoints
   * @param schema schema of candidate GeoPoint field
   * @return a decoder for GeoPoints
   */

  final def forGeopoints(schema: StructType): SearchDecoder = {

    StructTypeDecoder(
      Map(
        SearchIndexColumnImpl(
          GeoPointType.TYPE_LABEL,
          DataTypes.StringType,
          schema.fieldIndex(GeoPointType.TYPE_LABEL)
        ) -> AtomicDecoders.forUTF8Strings(),
        SearchIndexColumnImpl(
          GeoPointType.COORDINATES_LABEL,
          ArrayType(DataTypes.DoubleType),
          schema.fieldIndex(GeoPointType.COORDINATES_LABEL)
        ) -> ArrayDecoder(DataTypes.DoubleType, AtomicDecoders.identity())
      )
    )
  }
}