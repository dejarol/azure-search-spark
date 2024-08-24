package com.github.jarol.azure.search.spark.sql.connector.types.conversion

object CompatibilityRules
  extends RuleSet {

  final def allRules(): Set[SearchSparkConversionRule] = Set(
    DateTimeToDateRule
  )
}
