package com.github.jarol.azure.search.spark.sql.connector.types.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import org.apache.spark.sql.types.{DataType, DataTypes}

case object DateTimeToDateRule
  extends CompatibilityRule {

  override def sparkType(): DataType = DataTypes.DateType
  override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
}
