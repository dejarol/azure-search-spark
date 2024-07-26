package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartitioner

/**
 * Read configuration
 *
 * @param options options passed to the [[org.apache.spark.sql.DataFrameReader]]
 * @param configOptions read options retrieved from the underlying [[org.apache.spark.SparkConf]]
 */

case class ReadConfig(override protected val options: Map[String, String],
                      override protected val configOptions: Map[String, String])
  extends AbstractSearchConfig(options, configOptions, UsageMode.READ) {

  def partitioner: SearchPartitioner = {
    ???
  }
}

object ReadConfig {

  final val PARTITIONER_CLASS_CONFIG = "partitioner"
  final val PARTITIONER_CLASS_CONFIG_DEFAULT = classOf[SearchPartitioner].getName

  def apply(options: Map[String, String]): ReadConfig = {

    ReadConfig(
      options,
      AbstractSearchConfig.allConfigsFromActiveSessionForMode(UsageMode.READ)
    )
  }
}
