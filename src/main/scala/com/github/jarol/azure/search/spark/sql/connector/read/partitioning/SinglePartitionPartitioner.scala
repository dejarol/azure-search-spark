package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig

import java.util

/**
 * Simple partitioner that will generate a single partition
 * @param readConfig read config
 */

case class SinglePartitionPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  override def createPartitions(): util.List[SearchPartition] = {

    util.Collections.singletonList(
      ScalaSearchPartition(
        readConfig.filter,
        readConfig.select
      )
    )
  }
}
