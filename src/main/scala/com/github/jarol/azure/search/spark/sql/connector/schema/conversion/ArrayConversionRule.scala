package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.schema.SchemaUtils
import org.apache.spark.sql.types.{ArrayType, DataType}

case class ArrayConversionRule(private val sparkInternal: DataType,
                               private val searchInternal: SearchFieldDataType)
  extends InferSchemaRule {

  override def sparkType(): DataType = ArrayType(sparkInternal)
  override def searchType(): SearchFieldDataType = SearchFieldDataType.collection(searchInternal)
  override def converter(): SparkInternalConverter = ArrayConverter(searchInternal)
}

object ArrayConversionRule {

  def apply(searchInternal: SearchFieldDataType): ArrayConversionRule = {

    ArrayConversionRule(
      SchemaUtils.inferSparkTypeFor(
        new SearchField(null, searchInternal)
      ),
      searchInternal
    )
  }
}
