package com.github.jarol.azure.search.spark.sql.connector.types.conversion

trait CompatibilityRule
  extends SearchSparkConversionRule {

  override def useForSchemaInference(): Boolean = false

}
