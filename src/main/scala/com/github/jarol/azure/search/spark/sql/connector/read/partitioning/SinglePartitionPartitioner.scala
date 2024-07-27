package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig

case class SinglePartitionPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  override def generatePartitions(): Array[SearchPartition] = {

    Array(
      SinglePartition(readConfig)
    )
  }
}
