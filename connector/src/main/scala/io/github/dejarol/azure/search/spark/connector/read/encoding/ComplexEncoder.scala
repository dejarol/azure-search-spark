package io.github.dejarol.azure.search.spark.connector.read.encoding

import io.github.dejarol.azure.search.spark.connector.core.codec.{SearchIndexColumn, SearchIndexColumnImpl}
import io.github.dejarol.azure.search.spark.connector.core.schema.GeoPointType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructType}

import java.util.{Map => JMap}

/**
 * Encoder for complex (nested) objects
 * @param indexColumnToEncoders encoders to apply on complex object subfields
 */

case class ComplexEncoder(private val indexColumnToEncoders: Map[SearchIndexColumn, SearchEncoder])
  extends SearchEncoder {

  private lazy val sortedEncoders: Seq[(String, SearchEncoder)] = indexColumnToEncoders.toSeq.sortBy {
    case (k, _) => k.index()
  }.map {
    case (k, v) => (k.name(), v)
  }

  override def apply(value: Any): InternalRow = {

    val searchDocument: JMap[String, Object] = value.asInstanceOf[JMap[String, Object]]
    val values: Seq[Any] = sortedEncoders.map {
      case (name, encoder) => encoder.apply(searchDocument.get(name))
    }

    InternalRow.fromSeq(values)
  }
}

object ComplexEncoder {

  /**
   * Create a complex encoder for dealing with GeoPoints
   * @param schema schema of candidate GeoPoint field
   * @return an encoder for GeoPoints
   */

  final def forGeopoints(schema: StructType): SearchEncoder = {

    ComplexEncoder(
      Map(
        SearchIndexColumnImpl(
          GeoPointType.TYPE_LABEL,
          DataTypes.StringType,
          schema.fieldIndex(GeoPointType.TYPE_LABEL)
        ) -> AtomicEncoders.forUTF8Strings(),
        SearchIndexColumnImpl(
          GeoPointType.COORDINATES_LABEL,
          ArrayType(DataTypes.DoubleType),
          schema.fieldIndex(GeoPointType.COORDINATES_LABEL)
        ) -> CollectionEncoder(AtomicEncoders.identity())
      )
    )
  }
}
