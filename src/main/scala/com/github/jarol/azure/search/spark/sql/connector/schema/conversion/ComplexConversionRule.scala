package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import org.apache.spark.sql.types.{DataType, StructField, StructType}

case class ComplexConversionRule(private val fields: Seq[StructField],
                                 private val converters: Map[String, SparkInternalConverter])
  extends InferSchemaRule {

  override def sparkType(): DataType = StructType(fields)
  override def searchType(): SearchFieldDataType = SearchFieldDataType.COMPLEX
  override def converter(): SparkInternalConverter = ComplexConverter(converters)
}
