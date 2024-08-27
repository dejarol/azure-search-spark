package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import org.apache.spark.sql.types.DataType

trait SearchSparkConversionRuleSet {

  protected val ALL_RULES: Set[SearchSparkConversionRule]

  protected final def safelyCollectRule[T](predicate: SearchSparkConversionRule => Boolean,
                                           mapping: SearchSparkConversionRule => T): Option[T] = {

    ALL_RULES.collectFirst {
      case rule if predicate(rule) =>
        mapping(rule)
    }
  }

  final def safeRuleForTypes(spark: DataType, search: SearchFieldDataType): Option[SearchSparkConversionRule] = {

   safelyCollectRule(
     rule => rule.sparkType().equals(spark) && rule.searchType().equals(search),
     identity
   )
  }
}
