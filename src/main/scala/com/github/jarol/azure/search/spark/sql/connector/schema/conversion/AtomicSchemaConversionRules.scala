package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import org.apache.spark.sql.types.DataType

object AtomicSchemaConversionRules
  extends SearchSparkConversionRuleSet {

  override protected val ALL_RULES: Set[SearchSparkConversionRule] = Set(
    DateTimeToDateRule
  )

  final def existsRuleFor(sparkType: DataType, searchType: SearchFieldDataType): Boolean = safeRuleForTypes(sparkType, searchType).isDefined
}
