package com.github.jarol.azure.search.spark.sql.connector.types.conversion

import com.azure.search.documents.indexes.models.SearchFieldDataType
import org.apache.spark.sql.types.DataType

trait RuleSet {

  def allRules(): Set[SearchSparkConversionRule]

  protected final def collectFirst(predicate: SearchSparkConversionRule => Boolean): Option[SearchSparkConversionRule] = {

    allRules().collectFirst {
      case rule if predicate(rule) => rule
    }
  }

  final def safeRuleForTypes(sparkType: DataType, searchType: SearchFieldDataType): Option[SearchSparkConversionRule] = {

    collectFirst {
      rule => rule.sparkType().equals(sparkType) &&
        rule.searchType().equals(searchType)
    }
  }

  final def existsRuleForTypes(spark: DataType, search: SearchFieldDataType): Boolean = safeRuleForTypes(spark, search).isDefined
}
