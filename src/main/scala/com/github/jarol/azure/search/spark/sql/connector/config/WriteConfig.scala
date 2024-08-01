package com.github.jarol.azure.search.spark.sql.connector.config

/**
 * Write configuration
 * @param localOptions options passed to either a [[org.apache.spark.sql.DataFrameWriter]]
 * @param globalOptions write options retrieved from the underlying [[org.apache.spark.SparkConf]]
 */

case class WriteConfig(override protected val localOptions: Map[String, String],
                       override protected val globalOptions: Map[String, String])
  extends SearchIOConfig(localOptions, globalOptions, UsageMode.WRITE) {

  /**
   * Get the batch size to be used for writing documents along partitions
   * @return batch size for writing
   */

  def batchSize: Int = {

    getOrDefaultAs[Int](
      WriteConfig.BATCH_SIZE_CONFIG,
      WriteConfig.DEFAULT_BATCH_SIZE_VALUE,
      Integer.parseInt
    )
  }
}

object WriteConfig {

  final val BATCH_SIZE_CONFIG = "batchSize"
  final val DEFAULT_BATCH_SIZE_VALUE = 1000

  def apply(options: Map[String, String]): WriteConfig = {

    WriteConfig(
      options,
      SearchIOConfig.allConfigsFromActiveSessionForMode(UsageMode.WRITE)
    )
  }
}