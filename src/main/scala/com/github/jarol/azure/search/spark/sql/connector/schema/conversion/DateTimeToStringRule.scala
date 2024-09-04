package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import org.apache.spark.sql.types.{DataType, DataTypes}

case object DateTimeToStringRule
  extends SchemaConversionRule {

  override def sparkType(): DataType = DataTypes.StringType
  override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
  override def converter(): SparkInternalConverter = AtomicTypeConverters.StringConverter
}
