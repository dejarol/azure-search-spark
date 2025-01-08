package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.filter.ODataExpression

import java.util.{Collections => JColl, List => JList}

/**
 * Empty partitioner defined for testing purposes
 * @param readConfig read configuration
 * @param pushedPredicates predicates that support predicate pushdown
 */

case class EmptyPartitioner(
                             override protected val readConfig: ReadConfig,
                             override protected val pushedPredicates: Array[ODataExpression]
                           )
  extends AbstractSearchPartitioner(readConfig, pushedPredicates) {

  override def createPartitions(): JList[SearchPartition] = JColl.emptyList()
}
