package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsOperations._

abstract class AbstractSearchPartition(val partitionId: Int,
                                       val maybeFilter: Option[String],
                                       val maybeSelect: Option[Seq[String]])
  extends SearchPartition {

  override def getPartitionId: Int = partitionId

  override final def getSearchOptions: SearchOptions = {

    new SearchOptions()
      .setFilter(getFilter)
      .setSelect(maybeSelect)
  }
}
