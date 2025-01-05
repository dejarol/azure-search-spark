package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import org.apache.spark.sql.connector.expressions.filter.Predicate

/**
 * Simple Search partition
 * @param partitionId partition id
 * @param inputFilter optional filter to apply during data retrieval
 * @param maybeSelect optional list of index fields to select
 */

case class SimpleSearchPartition(
                                  override protected val partitionId: Int,
                                  override protected val inputFilter: Option[String],
                                  override protected val maybeSelect: Option[Seq[String]],
                                  override protected val pushedPredicates: Array[Predicate]
                                )
  extends AbstractSearchPartition(partitionId, inputFilter, maybeSelect, pushedPredicates) {

  override final protected[partitioning] def partitionFilter: Option[String] = None
}
