package io.github.dejarol.azure.search.spark.connector.read.partitioning

import java.util.{Collections => JCollections, List => JList}

/**
 * Simple partitioner that will generate a single partition
 */

case class DefaultPartitioner()
  extends SearchPartitioner {

  override def createPartitions(): JList[SearchPartition] = {

    JCollections.singletonList(
      DefaultSearchPartition(0)
    )
  }
}