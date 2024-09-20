package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

/**
 * Simple Search partition
 * @param partitionId partition id
 * @param inputFilter optional filter to apply during data retrieval
 * @param maybeSelect optional list of index fields to select
 */

case class SimpleSearchPartition(override protected val partitionId: Int,
                                 override protected val inputFilter: Option[String],
                                 override protected val maybeSelect: Option[Seq[String]])
  extends SearchPartitionTemplate(partitionId, inputFilter, maybeSelect) {

  override def getSearchFilter: String = inputFilter.orNull
}
