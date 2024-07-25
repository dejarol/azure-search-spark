package com.github.jarol.azure.search.spark.sql.connector.config

/**
 * Write configuration
 * @param options options passed to either a [[org.apache.spark.sql.DataFrameWriter]]
 */

case class WriteConfig(override protected val options: Map[String, String])
  extends AbstractSearchConfig(options, UsageMode.WRITE)
