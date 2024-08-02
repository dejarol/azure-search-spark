package com.github.jarol.azure.search.spark.sql.connector.config

import com.azure.search.documents.models.IndexActionType
import com.github.jarol.azure.search.spark.sql.connector.utils.Generics

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

    getOrDefaultAs(
      WriteConfig.BATCH_SIZE_CONFIG,
      WriteConfig.DEFAULT_BATCH_SIZE_VALUE,
      Integer.parseInt
    )
  }

  /**
   * Return the [[IndexActionType]] to use for indexing all documents
   * @return action type for indexing all documents
   */

  def actionType: Option[IndexActionType] = {

    getAs(
      WriteConfig.ACTION_CONFIG,
      WriteConfig.valueOfIndexActionType
    )
  }


}

object WriteConfig {

  final val BATCH_SIZE_CONFIG = "batchSize"
  final val DEFAULT_BATCH_SIZE_VALUE = 1000
  final val ACTION_CONFIG = "action"

  /**
   * Create an instance with options as local options
   * @param options local options
   * @return a write config instance with given local options and global options retrieved from the underlying [[org.apache.spark.SparkConf]]
   */

  def apply(options: Map[String, String]): WriteConfig = {

    WriteConfig(
      options,
      SearchIOConfig.allConfigsFromActiveSessionForMode(UsageMode.WRITE)
    )
  }

  /**
   * Retrieve the action type related to a value
   * @param value string value
   * @return the [[IndexActionType]] with same case-insensitive name or inner value
   */

  private def valueOfIndexActionType(value: String): IndexActionType = {

    Generics.unsafeValueOfEnum[IndexActionType](
      value,
      (v, s) => v.name().equalsIgnoreCase(s) || v.toString.equalsIgnoreCase(s)
    )
  }
}