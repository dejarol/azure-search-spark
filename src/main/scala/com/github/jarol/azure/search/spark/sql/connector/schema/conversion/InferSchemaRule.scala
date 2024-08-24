package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

trait InferSchemaRule
  extends SearchSparkConversionRule {

  override final def useForSchemaInference(): Boolean = true

  override def useForSchemaConversion(): Boolean = false
}
