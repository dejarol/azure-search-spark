package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig

import java.util

case class SinglePartitionPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  override def generatePartitions(): util.List[SearchPartition] = {

    util.Collections.singletonList(
      new SearchPartitionImpl(
        readConfig.filter.orNull,
        readConfig.select.map {
          JavaScalaConverters.seqToList
        }.orNull
      )
    )
  }
}
