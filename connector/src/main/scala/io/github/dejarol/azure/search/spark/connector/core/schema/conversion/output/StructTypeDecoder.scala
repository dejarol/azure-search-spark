package io.github.dejarol.azure.search.spark.connector.core.schema.conversion.output

import org.apache.spark.sql.catalyst.InternalRow

import java.util.{LinkedHashMap => JLinkedMap, Map => JMap}

/**
 * Converter for Spark internal rows
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
