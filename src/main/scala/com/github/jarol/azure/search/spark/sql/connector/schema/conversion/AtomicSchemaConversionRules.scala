package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import org.apache.spark.sql.types.DataType

object AtomicSchemaConversionRules {

  private lazy val ALL_CONVERSION_RULES: Set[SchemaConversionRule] = Set(
    DateTimeToDateRule
  )

  final def safeRuleFor(searchType: SearchFieldDataType, sparkType: DataType): Option[SchemaConversionRule] = {

    ALL_CONVERSION_RULES.collectFirst {
      case rule if rule.searchType().equals(searchType) &&
        rule.sparkType().equals(sparkType) => rule
    }
  }

  final def existsRuleFor(searchType: SearchFieldDataType, sparkType: DataType): Boolean = {

    safeRuleFor(
      searchType, sparkType
    ).isDefined
  }
}
