package com.github.jarol.azure.search.spark.sql.connector.config

/**
 * Write configuration
 * @param localOptions options passed to either a [[org.apache.spark.sql.DataFrameWriter]]
 * @param globalOptions write options retrieved from the underlying [[org.apache.spark.SparkConf]]
 */

case class WriteConfig(override protected val localOptions: Map[String, String],
                       override protected val globalOptions: Map[String, String])
  extends AbstractIOConfig(localOptions, globalOptions, UsageMode.WRITE)
