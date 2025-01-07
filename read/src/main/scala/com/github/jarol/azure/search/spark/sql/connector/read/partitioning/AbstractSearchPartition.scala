package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils
import com.github.jarol.azure.search.spark.sql.connector.read.filter.V2ExpressionAdapterFactory
import org.apache.spark.sql.connector.expressions.filter.Predicate

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
                                        protected val pushedPredicates: Array[Predicate]
                                      )
  extends SearchPartition {

  override final def getPartitionId: Int = partitionId

  override final def getODataFilter: String = {

    // Create the filter related to pushed predicates
    val oDataFiltersFromPredicates: Array[String] = pushedPredicates.map {
      V2ExpressionAdapterFactory.build
    }.collect {
      case Some(value) => value
    }

    val predicatesFilter: Option[String] = Option(
      StringUtils.createODataFilter(
        JavaScalaConverters.seqToList(oDataFiltersFromPredicates)
      )
    )

    // Combine input filter, partition filter and predicates filter
    StringUtils.createODataFilter(
      JavaScalaConverters.seqToList(
        Seq(inputFilter, partitionFilter, predicatesFilter).collect {
          case Some(value) => value
        }
      )
    )
  }

  override final def getPushedPredicates: Array[Predicate] = pushedPredicates

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
