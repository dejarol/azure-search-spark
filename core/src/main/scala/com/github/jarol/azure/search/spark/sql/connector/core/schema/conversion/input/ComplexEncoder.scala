package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.SearchIndexColumn
import org.apache.spark.sql.catalyst.InternalRow

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
