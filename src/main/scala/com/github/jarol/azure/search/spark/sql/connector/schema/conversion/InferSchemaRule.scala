package com.github.jarol.azure.search.spark.sql.connector.schema.conversion

/**
 * Conversion rules to be used for schema inference
 */

trait InferSchemaRule
  extends SearchSparkConversionRule {

  override final def useForSchemaInference(): Boolean = true

  override def useForSchemaConversion(): Boolean = false
}
