package io.github.jarol.azure.search.spark.connector.read.partitioning

import io.github.jarol.azure.search.spark.connector.read.config.ReadConfig
import java.util.{Collections => JColl, List => JList}

/**
 * Empty partitioner defined for testing purposes
 * @param readConfig read configuration
 */

case class EmptyPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  override def createPartitions(): JList[SearchPartition] = JColl.emptyList()
}
