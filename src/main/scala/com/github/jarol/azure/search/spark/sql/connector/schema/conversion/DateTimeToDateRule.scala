package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import org.apache.spark.sql.types.{DataType, DataTypes}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case object DateTimeToDateRule
  extends SchemaConversionRule {

  case object DateTimeToDateConverter
    extends SparkInternalConverter {
    override def toSparkInternalObject(value: Any): java.lang.Integer = {

      // Extract epoch day
      LocalDate.parse(
        value.asInstanceOf[String],
        DateTimeFormatter.ISO_DATE_TIME
      ).toEpochDay.toInt
    }
  }

  override def sparkType(): DataType = DataTypes.DateType
  override def searchType(): SearchFieldDataType = SearchFieldDataType.DATE_TIME_OFFSET
  override def converter(): SparkInternalConverter = DateTimeToDateConverter
}
