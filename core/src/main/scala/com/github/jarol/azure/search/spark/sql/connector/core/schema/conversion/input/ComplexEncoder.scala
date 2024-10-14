package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.FieldAdapter
import org.apache.spark.sql.catalyst.InternalRow

import java.util

/**
 * Encoder for complex (nested) objects
 * @param conversions encoders to apply on complex object subfields
 */

case class ComplexEncoder(private val conversions: Map[FieldAdapter, SearchEncoder])
  extends SearchEncoder {

  override def apply(value: Any): InternalRow = {

    val searchDocument: util.Map[String, Object] = value.asInstanceOf[util.Map[String, Object]]
    val values: Seq[Any] = conversions.map {
      case (k, converter) => converter.apply(searchDocument.get(k.name()))
    }.toSeq

    InternalRow.fromSeq(values)
  }
}
