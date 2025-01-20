package io.github.jarol.azure.search.spark.sql.connector.read.partitioning

import io.github.jarol.azure.search.spark.sql.connector.read.config.ReadConfig
import java.util.{Collections => JCollections, List => JList}

/**
 * Simple partitioner that will generate a single partition
 * @param readConfig read configuration
 */

case class DefaultPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  override def createPartitions(): JList[SearchPartition] = {

    JCollections.singletonList(
      DefaultSearchPartition(0)
    )
  }
}
