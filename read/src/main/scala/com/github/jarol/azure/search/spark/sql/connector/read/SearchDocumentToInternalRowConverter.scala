package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.SearchDocument
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.SparkInternalConverter
import org.apache.spark.sql.catalyst.InternalRow

case class SearchDocumentToInternalRowConverter(private val converters: Map[String, SparkInternalConverter])
  extends (SearchDocument => InternalRow) {

  override def apply(v1: SearchDocument): InternalRow = {

    val values: Seq[Any] = converters.map {
      case (name, converter) => converter.toSparkInternalObject(v1.get(name))
    }.toSeq
    InternalRow(values: _*)
  }
}
