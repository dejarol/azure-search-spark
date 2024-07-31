package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsOperations._

case class SearchPartitionImpl(filter: Option[String],
                               select: Option[Seq[String]])
  extends SearchPartition {

  override def getSearchOptions: SearchOptions = {

    new SearchOptions()
      .setFilter(filter)
      .setSelect(select)
  }
}
