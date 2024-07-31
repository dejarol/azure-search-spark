package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig

import java.util

case class SinglePartitionPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  override def createPartitions(): util.List[SearchPartition] = {

    util.Collections.singletonList(
      SearchPartitionImpl(
        readConfig.filter,
        readConfig.select
      )
    )
  }
}
