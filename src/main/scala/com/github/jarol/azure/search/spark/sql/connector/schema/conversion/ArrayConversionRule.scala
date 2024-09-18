package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input.{ArrayConverter, SparkInternalConverter}
import org.apache.spark.sql.types.{ArrayType, DataType}

case class ArrayConversionRule(private val sparkInternal: DataType,
                               private val searchInternal: SearchFieldDataType,
                               private val internalConverter: SparkInternalConverter)
  extends InferSchemaRule {

  override def sparkType(): DataType = ArrayType(sparkInternal)
  override def searchType(): SearchFieldDataType = SearchFieldDataType.collection(searchInternal)
  override def sparkConverter(): SparkInternalConverter = ArrayConverter(internalConverter)
}
