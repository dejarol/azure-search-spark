package com.github.jarol.azure.search.spark.sql.connector.config

/**
 * Read configuration
 * @param options options passed to the [[org.apache.spark.sql.DataFrameReader]]
 */

case class ReadConfig(override protected val options: Map[String, String])
  extends AbstractSearchConfig(options, UsageMode.READ) {

}

object ReadConfig {
}
