package com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input

import org.apache.spark.sql.catalyst.InternalRow

import java.util

/**
 * Converter for complex (nested) objects
 * @param conversions conversions to apply on complex object subfields
 */

case class ComplexConverter(private val conversions: Map[String, ReadConverter])
  extends ReadConverter {

  override def apply(value: Any): InternalRow = {

    val searchDocument: util.Map[String, Object] = value.asInstanceOf[util.Map[String, Object]]
    val values: Seq[Any] = conversions.map {
      case (k, converter) => converter.apply(searchDocument.get(k))
    }.toSeq

    InternalRow(values: _*)
  }
}
