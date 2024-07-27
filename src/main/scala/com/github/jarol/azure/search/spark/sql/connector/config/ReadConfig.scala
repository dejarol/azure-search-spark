package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{SearchPartitioner, SinglePartitionPartitioner}

/**
 * Read configuration
 * @param options options passed to the [[org.apache.spark.sql.DataFrameReader]]
 * @param sparkConfOptions read options retrieved from the underlying [[org.apache.spark.SparkConf]]
 */

case class ReadConfig(override protected val options: Map[String, String],
                      override protected val sparkConfOptions: Map[String, String])
  extends AbstractSearchConfig(options, sparkConfOptions, UsageMode.READ) {

  /**
   * Get the filter to apply on index documents.
   * The filter must follow OData syntax
   * ([[https://learn.microsoft.com/en-us/azure/search/search-query-odata-filter]])
   * @return the filter to apply on search index documents
   */

  def filter: Option[String] = safelyGet(ReadConfig.FILTER_CONFIG)

  /**
   * Get the [[SearchPartitioner]] to use for generating the search partitions.
   * If not provided, a [[SinglePartitionPartitioner]] will be used
   * @return a search partitioner instance
   */

  def partitioner: SearchPartitioner = {

    getAsOrDefault[SearchPartitioner](
      ReadConfig.PARTITIONER_CONFIG,
      SinglePartitionPartitioner(this),
      s => {
        ClassHelper.createInstance(
          Class.forName(s).asInstanceOf[Class[SearchPartitioner]]
        )
      }
    )
  }
}

object ReadConfig {

  final val FILTER_CONFIG = "filter"
  final val PARTITIONER_CONFIG = "partitioner"

  /**
   * Create an instance from given options, retrieving SparkConf-related options
   * from the underlying active [[org.apache.spark.sql.SparkSession]] (if any)
   * @param options options passed via [[org.apache.spark.sql.DataFrameReader.option]] method
   * @return an instance of [[ReadConfig]]
   */

  def apply(options: Map[String, String]): ReadConfig = {

    ReadConfig(
      options,
      AbstractSearchConfig.allConfigsFromActiveSessionForMode(UsageMode.READ)
    )
  }
}
