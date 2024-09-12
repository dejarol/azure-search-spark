package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import org.apache.spark.sql.catalyst.InternalRow

import java.util

/**
 * Converter for complex (nested) objects
 * @param conversions map with keys being nested attribute names and values being conversions to apply to nested attributes
 */

case class ComplexConverter(private val conversions: Map[String, SparkInternalConverter])
  extends SparkInternalConverter {

  override def toSparkInternalObject(value: Any): InternalRow = {

    val searchDocument: util.Map[String, Object] = value.asInstanceOf[util.Map[String, Object]]
    val values: Seq[Any] = conversions.map {
      case (k, converter) => converter.toSparkInternalObject(searchDocument.get(k))
    }.toSeq

    InternalRow(values: _*)
  }
}
