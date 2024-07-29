package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{SearchPartitioner, SinglePartitionPartitioner}

/**
 * Read configuration
 * @param localOptions options passed to the [[org.apache.spark.sql.DataFrameReader]]
 * @param globalOptions read options retrieved from the underlying [[org.apache.spark.SparkConf]]
 */

case class ReadConfig(override protected val localOptions: Map[String, String],
                      override protected val globalOptions: Map[String, String])
  extends AbstractIOConfig(localOptions, globalOptions, UsageMode.READ) {

  /**
   * Get the filter to apply on index documents. The filter must follow OData syntax
   * ([[https://learn.microsoft.com/en-us/azure/search/search-query-odata-filter]])
   * @return the filter to apply on search index documents
   */

  def filter: Option[String] = get(ReadConfig.FILTER_CONFIG)

  /**
   * Get the [[SearchPartitioner]] to use for generating the search partitions.
   * If not provided, a [[SinglePartitionPartitioner]] will be used
   * @return a search partitioner instance
   */

  def partitioner: SearchPartitioner = {

    getOrDefaultAs[SearchPartitioner](
      ReadConfig.PARTITIONER_CONFIG,
      SinglePartitionPartitioner(this),
      s => {
        ClassHelper.createInstance(
          Class.forName(s).asInstanceOf[Class[SearchPartitioner]],
          this
        )
      }
    )
  }

  def partitionerOptions: SearchConfig = getAllWithPrefix(ReadConfig.PARTITIONER_OPTIONS_PREFIX)

  /**
   * Return the set of index fields to select. If not provided, all retrievable fields will be selected
   * @return index fields to select
   */

  def select: Option[Seq[String]] = getAs(ReadConfig.SELECT_CONFIG, _.split(",").map(_.trim))
}

object ReadConfig {

  final val FILTER_CONFIG = "filter"
  final val PARTITIONER_CONFIG = "partitioner"
  final val SELECT_CONFIG = "select"
  final val PARTITIONER_OPTIONS_PREFIX = "partitioner.options."
  final val PARTITIONER_OPTIONS_FACET_CONFIG = "facet"
  final val PARTITIONER_OPTIONS_PARTITIONS_CONFIG = "partitions"

  /**
   * Create an instance from given options, retrieving SparkConf-related options
   * from the underlying active [[org.apache.spark.sql.SparkSession]] (if any)
   * @param options options passed via [[org.apache.spark.sql.DataFrameReader.option]] method
   * @return an instance of [[ReadConfig]]
   */

  def apply(options: Map[String, String]): ReadConfig = {

    ReadConfig(
      options,
      AbstractIOConfig.allConfigsFromActiveSessionForMode(UsageMode.READ)
    )
  }
}
