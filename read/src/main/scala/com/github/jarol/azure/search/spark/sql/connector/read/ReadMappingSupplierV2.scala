package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema._
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input._
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.{FieldAdapter, GeoPointRule, SafeMappingSupplierV2}
import org.apache.spark.sql.types.{DataType, DataTypes}

object ReadMappingSupplierV2
  extends SafeMappingSupplierV2[ReadConverter] {

  override protected[read] def forAtomicTypes(
                                               spark: DataType,
                                               search: SearchFieldDataType
                                             ): Option[ReadConverter] = {

    if (search.isString) {
      forString(spark)
    } else if (search.isNumeric) {
      forNumericTypes(search, spark)
    } else if (search.isBoolean) {
      forBoolean(spark)
    } else if (search.isDateTime) {
      forDateTime(spark)
    } else {
      None
    }
  }

  private def forString(dataType: DataType): Option[ReadConverter] = {

    dataType match {
      case DataTypes.StringType => Some(ReadConverters.UTF8_STRING)
      case _ => None
    }
  }

  private def forNumericTypes(
                               searchType: SearchFieldDataType,
                               dataType: DataType
                             ): Option[ReadConverter] = {

    if (dataType.isNumeric) {

      val numericMappingSupplier: Option[NumericCastingSupplier] = dataType match {
        case DataTypes.IntegerType => Some(NumericCastingSupplier.INT_32)
        case DataTypes.LongType => Some(NumericCastingSupplier.INT_64)
        case DataTypes.DoubleType => Some(NumericCastingSupplier.DOUBLE)
        case DataTypes.FloatType => Some(NumericCastingSupplier.SINGLE)
        case _ => None
      }

      numericMappingSupplier.flatMap {
        _.get(searchType)
      }
    } else {
      dataType match {
        case DataTypes.StringType => Some(ReadConverters.STRING_VALUE_OF.andThen(ReadConverters.UTF8_STRING))
        case _ => None
      }
    }
  }

  private def forBoolean(dataType: DataType): Option[ReadConverter] = {

    dataType match {
      case DataTypes.StringType => Some(ReadConverters.STRING_VALUE_OF.andThen(ReadConverters.UTF8_STRING))
      case DataTypes.BooleanType => Some(ReadConverters.BOOLEAN)
      case _ => None
    }
  }

  private def forDateTime(dataType: DataType): Option[ReadConverter] = {

    dataType match {
      case DataTypes.TimestampType => Some(ReadConverters.TIMESTAMP)
      case DataTypes.DateType => Some(ReadConverters.DATE)
      case DataTypes.StringType => Some(ReadConverters.UTF8_STRING)
      case _ => None
    }
  }

  override protected def forCollection(sparkType: DataType, search: SearchField, internal: ReadConverter): ReadConverter = CollectionConverter(internal)
  override protected def forComplex(internal: Map[FieldAdapter, ReadConverter]): ReadConverter = ComplexConverter(Map.empty)
  override protected def forGeoPoint: ReadConverter = GeoPointRule.readConverter()
}
