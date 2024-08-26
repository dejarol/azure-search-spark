package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import org.apache.spark.sql.types.DataType

object SchemaConversionRules
  extends Set[SchemaConversionRule] {

  private lazy val ALL_CONVERSION_RULES: Set[SchemaConversionRule] = Set(
    DateTimeToDateRule
  )

  override def contains(elem: SchemaConversionRule): Boolean = ALL_CONVERSION_RULES.contains(elem)

  override def +(elem: SchemaConversionRule): Set[SchemaConversionRule] = ALL_CONVERSION_RULES + elem

  override def -(elem: SchemaConversionRule): Set[SchemaConversionRule] = ALL_CONVERSION_RULES - elem

  override def iterator: Iterator[SchemaConversionRule] = ALL_CONVERSION_RULES.iterator

  final def existsRuleFor(searchType: SearchFieldDataType, sparkType: DataType): Boolean = {

    exists {
      rule => rule.searchType().equals(searchType) &&
        rule.sparkType().equals(sparkType)
    }
  }
}
