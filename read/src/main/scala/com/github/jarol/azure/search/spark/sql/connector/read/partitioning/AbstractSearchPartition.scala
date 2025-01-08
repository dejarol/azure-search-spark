package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.read.filter.ODataExpression

import java.util.{Collections => JColl, List => JList}

/**
 * Parent class for Scala-based [[SearchPartition]](s)
 * @param partitionId partition id
 * @param inputFilter optional filter to apply during data retrieval
 * @param maybeSelect optional list of index fields to select
 * @param pushedPredicates predicates that support pushdown
 */

abstract class AbstractSearchPartition(
                                        protected val partitionId: Int,
                                        protected val inputFilter: Option[String],
                                        protected val maybeSelect: Option[Seq[String]],
                                        protected val pushedPredicates: Seq[ODataExpression]
                                      )
  extends SearchPartition {

  override final def getPartitionId: Int = partitionId

  override final def getODataFilter: String = {

    // Create the filter related to pushed predicates
    val predicatesFilter: Option[String] = AbstractSearchPartition.createODataFilter(pushedPredicates.map(_.toUriLiteral))

    // Combine input filter, partition filter and predicates filter
    AbstractSearchPartition.createODataFilter(
      Seq(inputFilter, partitionFilter, predicatesFilter).collect {
        case Some(value) => value
      }
    ).orNull
  }

  override final def getPushedPredicates: Array[ODataExpression] = pushedPredicates.toArray

  override final def getSelectedFields: JList[String] = {

    maybeSelect.map {
      JavaScalaConverters.seqToList
    }.getOrElse {
      JColl.emptyList[String]()
    }
  }

  /**
   * Get the partition-specific filter to be applied by this instance
   * @return the partition filter for this instance
   */

  protected[partitioning] def partitionFilter: Option[String]
}

object AbstractSearchPartition {

  /**
   * Combine a collection of filters into a single OData filter
   * @param filters filters to combine
   * @return an overall filters that combine the input ones using <code>and</code>
   */

  private[partitioning] def createODataFilter(filters: Seq[String]): Option[String] = {

    if (filters.isEmpty) {
      None
    } else if (filters.size.equals(1)) {
      filters.headOption
    } else {
      Some(
        filters.map {
          filter => s"($filter)"
        }.mkString(" and ")
      )
    }
  }
}
