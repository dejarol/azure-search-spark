package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsOperations._

case class ScalaSearchPartition(override val maybeFilter: Option[String],
                                override val maybeSelect: Option[Seq[String]])
  extends AbstractScalaSearchPartition(maybeFilter, maybeSelect) {

  override def getSearchOptions: SearchOptions = {

    new SearchOptions()
      .setFilter(maybeFilter)
      .setSelect(maybeSelect)
  }
}
