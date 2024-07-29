package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsOperations._

case class SinglePartition(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartition(readConfig) {

  override def getSearchOptions: SearchOptions = {

    new SearchOptions()
      .setFilter(readConfig.filter)
      .setSelect(readConfig.select)
  }
}
