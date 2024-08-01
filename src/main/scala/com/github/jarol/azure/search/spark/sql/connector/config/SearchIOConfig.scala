package com.github.jarol.azure.search.spark.sql.connector.config

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Parent class for read/write configurations
 * @param localOptions options passed to either a [[org.apache.spark.sql.DataFrameReader]] (when used in [[UsageMode.READ]])
 *                or [[org.apache.spark.sql.DataFrameWriter]] (when used in [[UsageMode.WRITE]])
 * @param globalOptions all options related to the config usage mode, retrieved from the underlying [[SparkConf]] (if any)
 * @param usageMode usage mode
 */

class SearchIOConfig(override protected val localOptions: Map[String, String],
                     override protected val globalOptions: Map[String, String],
                     protected val usageMode: UsageMode)
  extends SearchConfig(localOptions, globalOptions)
    with IOConfig {

  override def getEndpoint: String = unsafelyGet(IOConfig.END_POINT_CONFIG)

  override def getAPIkey: String = unsafelyGet(IOConfig.API_KEY_CONFIG)

  override def getIndex: String = unsafelyGet(IOConfig.INDEX_CONFIG)
}

object SearchIOConfig {

  /**
   * Extract all options from given SparkConf that starts with the prefix of a usage mode
   * @param sparkConf an instance of [[SparkConf]]
   * @param mode usage mode
   * @return all key-value pairs whose keys start with given mode prefix
   */

  protected[config] def allConfigsForMode(sparkConf: SparkConf, mode: UsageMode): Map[String, String] = {

   sparkConf
      .getAllWithPrefix(mode.prefix())
      .toMap
  }

  /**
   * Retrieve all options related to a mode from the active SparkSession (if any)
   * @param mode usage mode
   * @return an empty Map if no [[SparkSession]] is active, all options related to the mode otherwise
   */

  protected[config] def allConfigsFromActiveSessionForMode(mode: UsageMode): Map[String, String] = {

    SparkSession.getActiveSession match {
      case Some(value) => allConfigsForMode(value.sparkContext.getConf, mode)
      case None => Map.empty
    }
  }
}
