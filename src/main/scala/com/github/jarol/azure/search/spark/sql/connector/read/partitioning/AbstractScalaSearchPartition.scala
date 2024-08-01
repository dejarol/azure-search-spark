package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters

import java.util

abstract class AbstractScalaSearchPartition(val partitionId: Int,
                                            val maybeFilter: Option[String],
                                            val maybeSelect: Option[Seq[String]])
  extends SearchPartition {

  override def getPartitionId: Int = partitionId

  override def getFilter: String = maybeFilter.orNull

  override def getSelect: util.List[String] = {

    maybeSelect.map {
      JavaScalaConverters.seqToList
    }.orNull
  }
}
