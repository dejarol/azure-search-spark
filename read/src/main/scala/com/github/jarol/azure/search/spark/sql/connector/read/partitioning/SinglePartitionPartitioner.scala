package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.filter.ODataExpression

import java.util.{Collections => JCollections, List => JList}

/**
 * Simple partitioner that will generate a single partition
 * @param readConfig read configuration
 * @param pushedPredicates predicates that support predicate pushdown
 */

case class SinglePartitionPartitioner(
                                       override protected val readConfig: ReadConfig,
                                       override protected val pushedPredicates: Array[ODataExpression]
                                     )
  extends AbstractSearchPartitioner(readConfig, pushedPredicates) {

  override def createPartitions(): JList[SearchPartition] = {

    JCollections.singletonList(
      SimpleSearchPartition(
        0,
        readConfig.filter,
        readConfig.select,
        pushedPredicates
      )
    )
  }
}
