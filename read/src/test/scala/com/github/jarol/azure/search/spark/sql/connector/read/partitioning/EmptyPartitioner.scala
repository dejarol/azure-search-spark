package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig

import java.util.{Collections => JColl, List => JList}

/**
 * Empty partitioner defined for testing purposes
 * @param readConfig read configuration
 */

case class EmptyPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  override def createPartitions(): JList[SearchPartition] = JColl.emptyList()
}
