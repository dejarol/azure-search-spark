package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

case class ScalaSearchPartition(override val partitionId: Int,
                                override val maybeFilter: Option[String],
                                override val maybeSelect: Option[Seq[String]])
  extends AbstractSearchPartition(partitionId, maybeFilter, maybeSelect) {

  override def getFilter: String = maybeFilter.orNull
}
