package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import org.apache.spark.sql.catalyst.InternalRow

import java.util.Objects

case class ComplexConverter(private val conversions: Map[String, SparkInternalConverter])
  extends SparkInternalConverter {

  override def toSparkInternalObject(value: Any): InternalRow = {

    val searchDocument: java.util.Map[String, Object] = value.asInstanceOf[java.util.Map[String, Object]]
    val values: Seq[Any] = conversions.collect {
      case (k, converter) if Objects.nonNull(searchDocument.get(k)) =>
        converter.toSparkInternalObject(searchDocument.get(k))
    }.toSeq

    InternalRow(values: _*)
  }
}
