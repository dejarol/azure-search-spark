package com.github.jarol.azure.search.spark.sql.connector.types.conversion

trait SchemaConversionRule
  extends SearchSparkConversionRule {

  override def useForSchemaInference(): Boolean = false

  override def useForSchemaConversion(): Boolean = true
}
