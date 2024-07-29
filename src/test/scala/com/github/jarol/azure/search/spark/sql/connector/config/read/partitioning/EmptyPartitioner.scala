package com.github.jarol.azure.search.spark.sql.connector.config.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{AbstractSearchPartitioner, SearchPartition}

/**
 * Empty partitioner defined for testing purposes
 * @param readConfig read configuration
 */

case class EmptyPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  override def generatePartitions(): Array[SearchPartition] = Array.empty
}
