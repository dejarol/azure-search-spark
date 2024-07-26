package com.github.jarol.azure.search.spark.sql.connector.config

/**
 * Write configuration
 * @param options options passed to either a [[org.apache.spark.sql.DataFrameWriter]]
 * @param configOptions write options retrieved from the underlying [[org.apache.spark.SparkConf]]
 */

case class WriteConfig(override protected val options: Map[String, String],
                       override protected val configOptions: Map[String, String])
  extends AbstractSearchConfig(options, configOptions, UsageMode.WRITE)
