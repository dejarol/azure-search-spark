package io.github.dejarol.azure.search.spark.connector.read.filter

import io.github.dejarol.azure.search.spark.connector.core.Constants
import io.github.dejarol.azure.search.spark.connector.core.utils.StringUtils

import java.sql.{Date, Timestamp}

/**
 * Trait for converting a constant value to a String to use on
 * an OData <code>$filter</code> expression
 * @tparam T value type
 */

sealed trait ODataLiterable[T] {

  /**
   * Converts an instance of given type to an OData literal
   * @param value value to convert
   * @return an OData literal
   */

  def toLiteral(value: T): String
}

object ODataLiterables {

  /**
   * Implementation for strings
   */

  private[filter] implicit object StringLiterable
    extends ODataLiterable[String] {
    override def toLiteral(value: String): String = StringUtils.singleQuoted(value)
  }

  /**
   * Creates a literable implementation for a numeric type
   * <br>
   * The implementation will simply invoke [[String.valueOf]] on a value
   * @tparam T numeric type (should extend [[Number]])
   * @return a literable implementation for a numeric type
   */

  private[filter] def numericLiterable[T <: Number]: ODataLiterable[T] = {

    new ODataLiterable[T] {
      override def toLiteral(value: T): String =
        String.valueOf(value)
    }
  }

  /**
   * Implementation for dates
   */

  private[filter] implicit object DateLiterable
    extends ODataLiterable[Date] {

    override def toLiteral(value: Date): String = {

      value.toLocalDate
        .atStartOfDay(Constants.UTC_OFFSET)
        .format(Constants.DATETIME_OFFSET_FORMATTER)
    }
  }

  /**
   * Implementation for timestamps
   */

  private[filter] implicit object TimestampLiterable
    extends ODataLiterable[Timestamp] {

    override def toLiteral(value: Timestamp): String = {

      value.toInstant
        .atOffset(Constants.UTC_OFFSET)
        .format(Constants.DATETIME_OFFSET_FORMATTER)
    }
  }
}
