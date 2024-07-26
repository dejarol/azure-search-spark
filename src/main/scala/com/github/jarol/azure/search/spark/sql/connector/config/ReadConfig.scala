package com.github.jarol.azure.search.spark.sql.connector.config

import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{SearchPartitioner, SinglePartitionPartitioner}

/**
 * Read configuration
 *
 * @param options options passed to the [[org.apache.spark.sql.DataFrameReader]]
 * @param sparkConfOptions read options retrieved from the underlying [[org.apache.spark.SparkConf]]
 */

case class ReadConfig(override protected val options: Map[String, String],
                      override protected val sparkConfOptions: Map[String, String])
  extends AbstractSearchConfig(options, sparkConfOptions, UsageMode.READ) {

  def partitioner: SearchPartitioner = {

    getAsOrDefault[SearchPartitioner](
      ReadConfig.PARTITIONER_CONFIG,
      ReadConfig.PARTITIONER_DEFAULT,
      s => {
        ClassHelper.createInstance(
          Class.forName(s).asInstanceOf[Class[SearchPartitioner]]
        )
      }
    )
  }
}

object ReadConfig {

  final val PARTITIONER_CONFIG = "partitioner"
  final lazy val PARTITIONER_DEFAULT = new SinglePartitionPartitioner()

  def apply(options: Map[String, String]): ReadConfig = {

    ReadConfig(
      options,
      AbstractSearchConfig.allConfigsFromActiveSessionForMode(UsageMode.READ)
    )
  }
}
