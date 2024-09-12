package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters

import java.util

/**
 * Parent class for Scala-based [[SearchPartition]](s)
 * @param partitionId partition id
 * @param inputFilter optional filter to apply during data retrieval
 * @param maybeSelect optional list of index fields to select
 */

abstract class AbstractScalaSearchPartition(protected val partitionId: Int,
                                            protected val inputFilter: Option[String],
                                            protected val maybeSelect: Option[Seq[String]])
  extends SearchPartition {

  override final def getPartitionId: Int = partitionId

  override final def getSearchSelect: util.List[String] = {

    maybeSelect.map {
      JavaScalaConverters.seqToList
    }.getOrElse {
      util.Collections.emptyList[String]()
    }
  }
}
