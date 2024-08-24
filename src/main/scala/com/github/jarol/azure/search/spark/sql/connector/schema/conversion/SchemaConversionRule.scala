package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

trait SchemaConversionRule
  extends SearchSparkConversionRule {

  override def useForSchemaInference(): Boolean = false

  override def useForSchemaConversion(): Boolean = true
}
