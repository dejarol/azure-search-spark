package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema._
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input._
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{FieldAdapter, GeoPointRule, SafeMappingSupplierV2}
import org.apache.spark.sql.types.{DataType, DataTypes, NumericType}

object ReadMappingSupplierV2
  extends SafeMappingSupplierV2[ReadConverter] {

  override protected[read] def forAtomicTypes(
                                               spark: DataType,
                                               search: SearchFieldDataType
                                       ): Option[ReadConverter] = {

    spark match {
      case DataTypes.StringType => forString(search)
      case numeric: NumericType => forNumericTypes(numeric, search)
      case DataTypes.BooleanType => forBoolean(search)
      case DataTypes.DateType => forDate(search)
      case DataTypes.TimestampType => forTimestamp(search)
      case _ => None
    }
  }

  private def forString(searchType: SearchFieldDataType): Option[ReadConverter] = {

    if (searchType.isString || searchType.isDateTime) {
      Some(ReadTransformConverter.UTF8_STRING)
    } else if (searchType.isNumeric || searchType.isBoolean) {
      Some(ReadTransformConverter.STRING_VALUE_OF
        .andThen(ReadTransformConverter.UTF8_STRING)
      )
    } else {
      None
    }
  }

  private def forNumericTypes(numericType: NumericType, searchType: SearchFieldDataType): Option[ReadConverter] = {

    if (searchType.isNumeric) {

      val numericMappingSupplier: Option[NumericCastingSupplier] = numericType match {
        case DataTypes.IntegerType => Some(NumericCastingSupplier.Integer)
        case DataTypes.LongType => Some(NumericCastingSupplier.Long)
        case DataTypes.DoubleType => Some(NumericCastingSupplier.Double)
        case DataTypes.FloatType => Some(NumericCastingSupplier.Float)
        case _ => None
      }

      numericMappingSupplier.flatMap {
        _.get(searchType)
      }
    } else {
      None
    }
  }

  private def forBoolean(searchType: SearchFieldDataType): Option[ReadConverter] = {

    if (searchType.isString) {
      Some(ReadTransformConverter.STRING_VALUE_OF)
    } else if (searchType.isBoolean) {
      Some(ReadCastConverter.BOOLEAN)
    } else {
      None
    }
  }

  private def forDate(searchType: SearchFieldDataType): Option[ReadConverter] = {

    if (searchType.isDateTime) {
      Some(ReadTimeConverter.DATE)
    } else {
      None
    }
  }

  private def forTimestamp(searchType: SearchFieldDataType): Option[ReadConverter] = {

    if (searchType.isDateTime) {
      Some(ReadTimeConverter.TIMESTAMP)
    } else {
      None
    }
  }

  override protected def forCollection(sparkType: DataType, search: SearchField, internal: ReadConverter): ReadConverter = CollectionConverter(internal)
  override protected def forComplex(internal: Map[FieldAdapter, ReadConverter]): ReadConverter = ComplexConverter(Map.empty)
  override protected def forGeoPoint: ReadConverter = GeoPointRule.readConverter()
}
