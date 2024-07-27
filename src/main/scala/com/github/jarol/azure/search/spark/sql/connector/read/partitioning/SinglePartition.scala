package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig

case class SinglePartition(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartition(readConfig) {

  override def getSearchOptions: SearchOptions = {

    val searchOptions = new SearchOptions()
    setFilter(searchOptions, readConfig.filter)
  }
}
