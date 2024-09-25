package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.core.DataTypeException
import com.github.jarol.azure.search.spark.sql.connector.core.schema._

object FilterValueFormatters {

  object StringFormatter extends FilterValueFormatter {
    override def format(value: Any): String = {
      s"'${value.asInstanceOf[String]}'"
    }
  }

  object NumericFormatter extends FilterValueFormatter {
    override def format(value: Any): String = String.valueOf(value)
  }

  object DateTimeFormatter extends FilterValueFormatter {
    override def format(value: Any): String = s"'${String.valueOf(value)}'"
  }

  /**
   * Retrieve the proper formatter for a given search field type
   * @param `type` search field type
   * @throws DataTypeException for unsupported types
   * @return a filter value formatter
   */

  @throws[DataTypeException]
  def forType(`type`: SearchFieldDataType): FilterValueFormatter = {

    if (`type`.isString) {
      StringFormatter
    } else if (`type`.isNumeric) {
      NumericFormatter
    } else if (`type`.isDateTime) {
      DateTimeFormatter
    } else {
      throw new DataTypeException(f"No formatter defined for data type ${`type`}")
    }
  }
}
