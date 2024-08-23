package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.SearchDocument
import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataTypes, StructType}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, ZoneOffset}
import java.util

case class InternalRowToSearchDocumentConverter(private val schema: StructType)
  extends (InternalRow => SearchDocument) {

  override def apply(v1: InternalRow): SearchDocument = {

    val properties: util.Map[String, Any] = new util.HashMap()
    schema.zipWithIndex.foreach {
      case (sf, index) =>
        if (!v1.isNullAt(index)) {

          val propertyValue = sf.dataType match {
            case DataTypes.StringType => v1.getString(index)
            case DataTypes.IntegerType => v1.getInt(index)
            case DataTypes.LongType => v1.getLong(index)
            case DataTypes.DoubleType => v1.getDouble(index)
            case DataTypes.FloatType => v1.getFloat(index)
            case DataTypes.BooleanType => v1.getBoolean(index)
            case DataTypes.DateType => LocalDate.ofEpochDay(v1.getInt(index))
              .atTime(0, 0, 0)
              .atOffset(ZoneOffset.UTC)
              .format(DateTimeFormatter.ISO_DATE_TIME)
            case DataTypes.TimestampType => Timestamp.from(
              Instant.EPOCH.plus(v1.getLong(index),
                ChronoUnit.MICROS)
            ).toLocalDateTime.atOffset(
              ZoneOffset.UTC
            ).format(
              DateTimeFormatter.ISO_DATE_TIME
            )
            case struct: StructType => apply(v1.getStruct(index, struct.size))
            case _ => throw new AzureSparkException("Unsupported type for write")
          }
          properties.put(sf.name, propertyValue)
        }
    }
    new SearchDocument(properties)
  }
}
