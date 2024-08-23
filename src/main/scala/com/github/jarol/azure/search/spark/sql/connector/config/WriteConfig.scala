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
  extends SearchIOConfig(localOptions, globalOptions) {

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
   * Return the (optional) [[IndexActionType]] to use for indexing all documents
   * @return action type for indexing all documents
   */

  def maybeUserSpecifiedAction: Option[IndexActionType] = {

    getAs(
      WriteConfig.ACTION_CONFIG,
      WriteConfig.valueOfIndexActionType
    )
  }

  /**
   * Return the [[IndexActionType]] defined by the user or a default. It will be used for indexing all documents.
   * If not specified, [[WriteConfig.DEFAULT_ACTION_TYPE]] will be used
   * @return action type for indexing all documents
   */

  def overallAction: IndexActionType = maybeUserSpecifiedAction.getOrElse(WriteConfig.DEFAULT_ACTION_TYPE)

  /**
   * Return the name of a dataframe column that contains a per-document action type.
   * It must be the name of an existing string column whose values can be mapped to an [[IndexActionType]]
   * @return column name for document action
   */

  def actionColumn: Option[String] = get(WriteConfig.ACTION_COLUMN_CONFIG)
}

object WriteConfig {

  final val BATCH_SIZE_CONFIG = "batchSize"
  final val DEFAULT_BATCH_SIZE_VALUE = 1000
  final val ACTION_CONFIG = "action"
  final val ACTION_COLUMN_CONFIG = "actionColumn"
  final val DEFAULT_ACTION_TYPE: IndexActionType = IndexActionType.MERGE_OR_UPLOAD

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