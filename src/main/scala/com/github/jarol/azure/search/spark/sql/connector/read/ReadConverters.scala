package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.SearchDocument
import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, JavaScalaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.lang
import java.util
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalDateTime}

/**
 * Object that collects all available read converters
 */

object ReadConverters {

  /**
   * Converter for strings.
   *
   * As we are operating with [[InternalRow]](s), it should convert a simple string to an [[UTF8String]]
   */

  case object StringConverter
    extends ReadTransformConverter[UTF8String] {

    override protected def transform(obj: Object): UTF8String = {
      UTF8String.fromString(obj.asInstanceOf[String])
    }
  }

  /**
   * Converter for integers
   */

  case object IntegerConverter
    extends ReadCastingConverter[Integer]

  /**
   * Converter for longs
   */

  case object LongConverter
    extends ReadCastingConverter[lang.Long]

  /**
   * Converter for double
   */

  case object DoubleConverter
    extends ReadCastingConverter[lang.Double]

  /**
   * Converter for floats
   */

  case object FloatConverter
    extends ReadCastingConverter[lang.Float]

  /**
   * Converter for booleans
   */

  case object BooleanConverter
    extends ReadCastingConverter[lang.Boolean]

  /**
   * Converter for dates.
   *
   * As we are operating with [[InternalRow]](s),
   * it should convert a date formatted as string into its epoch days
   * (i.e. the number of days past since 1970-01-01)
   */

  case object DateConverter
    extends ReadTransformConverter[Integer] {

    override protected def transform(obj: Object): Integer = {

      // Extract epoch day
      LocalDate.parse(
        obj.asInstanceOf[String],
        DateTimeFormatter.ISO_DATE_TIME
      ).toEpochDay.toInt
    }
  }

  /**
   * Converter for timestamps
   *
   * As we are operating with [[InternalRow]](s), it should convert a timestamp formatted as string
   * into its epoch microseconds (i.e. the number of microseconds past since 1970-01-01T00:00:00)
   */

  case object TimestampConverter
    extends ReadTransformConverter[lang.Long] {

    override protected def transform(obj: Object): lang.Long = {

      // Convert string datetime to instant
      val datetimeInstant: Instant = Timestamp.valueOf(
        LocalDateTime.parse(
          obj.asInstanceOf[String],
          DateTimeFormatter.ISO_DATE_TIME
        )
      ).toInstant

      // Compute number of microseconds since epoch
      ChronoUnit.MICROS.between(
        Instant.EPOCH,
        datetimeInstant
      )
    }
  }

  case class ArrayConverter(sparkElementType: DataType)
    extends ReadTransformConverter[ArrayData] {

    override protected def transform(obj: Object): ArrayData = {

      val readConverter: ReadConverter[_] = converterForType(sparkElementType)
      val arrayValues: Seq[Any] = JavaScalaConverters.listToSeq(
        obj.asInstanceOf[util.List[Object]]
      ).map {
        readConverter(_)
      }

      ArrayData.toArrayData(arrayValues)
    }
  }

  case class ComplexConverter(schema: StructType)
    extends ReadTransformConverter[InternalRow] {

    override protected def transform(obj: Object): InternalRow = {

      val subDocument = new SearchDocument(obj.asInstanceOf[java.util.Map[String, Object]])
      SearchDocumentToInternalRowConverter(schema).apply(subDocument)
    }
  }

  def converterForType(dType: DataType): ReadConverter[_] = {

    dType match {
      case DataTypes.StringType => ReadConverters.StringConverter
      case DataTypes.IntegerType => ReadConverters.IntegerConverter
      case DataTypes.LongType => ReadConverters.LongConverter
      case DataTypes.DoubleType => ReadConverters.DoubleConverter
      case DataTypes.FloatType => ReadConverters.FloatConverter
      case DataTypes.BooleanType => ReadConverters.BooleanConverter
      case DataTypes.DateType => ReadConverters.DateConverter
      case DataTypes.TimestampType => ReadConverters.TimestampConverter
      case struct: StructType => ReadConverters.ComplexConverter(struct)
      case array: ArrayType => ReadConverters.ArrayConverter(array.elementType)
      case _ => throw new AzureSparkException(s"No conversion defined for datatype ($dType)")
    }
  }
}
