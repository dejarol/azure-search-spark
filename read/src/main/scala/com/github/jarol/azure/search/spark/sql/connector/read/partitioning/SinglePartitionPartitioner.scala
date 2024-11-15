package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig

import java.util.{Collections => JCollections, List => JList}

/**
 * Simple partitioner that will generate a single partition
 * @param readConfig read config
 */

case class SinglePartitionPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  override def createPartitions(): JList[SearchPartition] = {

    JCollections.singletonList(
      SimpleSearchPartition(
        0,
        readConfig.filter,
        readConfig.select
      )
    )
  }
}
