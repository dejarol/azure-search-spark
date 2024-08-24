package com.github.jarol.azure.search.spark.sql.connector.types.conversion

trait InferSchemaRule
  extends SearchSparkConversionRule {

  override final def useForSchemaInference(): Boolean = true
}
