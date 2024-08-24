package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.schema.SchemaUtils
import org.apache.spark.sql.types.{DataType, StructField, StructType}

case class ComplexConversionRule(private val fields: Seq[StructField],
                                 private val converters: Map[String, SparkInternalConverter])
  extends InferSchemaRule {

  override def sparkType(): DataType = StructType(fields)

  override def searchType(): SearchFieldDataType = SearchFieldDataType.COMPLEX

  override def converter(): SparkInternalConverter = ComplexConverter(converters)
}

object ComplexConversionRule {

  def apply(fields: java.util.List[SearchField]): ComplexConversionRule = {

    val fieldSeq: Seq[SearchField] = JavaScalaConverters.listToSeq(fields)
    ComplexConversionRule(
      fieldSeq.map(SchemaUtils.asStructField),
      Map.empty
    )
  }
}
