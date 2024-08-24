package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

object SchemaConversionRules
  extends RuleSet {

  final def allRules(): Set[SearchSparkConversionRule] = Set(
    DateTimeToDateRule
  )
}
