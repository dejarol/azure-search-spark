package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchFieldDataType
import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException

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

  private def isNumber(`type`: SearchFieldDataType): Boolean = {

    Seq(
      SearchFieldDataType.INT32,
      SearchFieldDataType.INT64,
      SearchFieldDataType.DOUBLE,
      SearchFieldDataType.SINGLE
    ).exists {
      _.equals(`type`)
    }
  }

  /**
   * Retrieve the proper formatter for a given search field type
   * @param `type` search field type
   * @throws AzureSparkException for unsupported types
   * @return a filter value formatter
   */

  @throws[AzureSparkException]
  def forType(`type`: SearchFieldDataType): FilterValueFormatter = {

    if (`type`.equals(SearchFieldDataType.STRING)) {
      StringFormatter
    } else if (isNumber(`type`)) {
      NumericFormatter
    } else if (`type`.equals(SearchFieldDataType.DATE_TIME_OFFSET)) {
      DateTimeFormatter
    } else {
      throw new AzureSparkException(f"Data type ${`type`} cannot not be formatted")
    }
  }
}
