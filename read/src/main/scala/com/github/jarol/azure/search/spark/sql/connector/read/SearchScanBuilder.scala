package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.core.IndexDoesNotExistException
import com.github.jarol.azure.search.spark.sql.connector.read.filter.PredicateParser
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType

/**
 * Scan builder for Search DataSource
 * @param readConfig read configuration
 * @param schema index schema (either inferred or defined by the user)
 */

class SearchScanBuilder(
                         private val readConfig: ReadConfig,
                         private val schema: StructType
                       )
  extends ScanBuilder
    with SupportsPushDownV2Filters {

  private var supportedPredicates: Option[Array[Predicate]] = None

  /**
   * Build the scan
   * @throws IndexDoesNotExistException if the target index does not exist
   * @return a scan to be used for Search DataSource
   */

  @throws[IndexDoesNotExistException]
  override def build(): Scan = {

    if (!readConfig.indexExists) {
      throw new IndexDoesNotExistException(readConfig.getIndex)
    } else {
      new SearchScan(readConfig, schema)
    }
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {

    if (readConfig.pushdownPredicate) {
      val (supported, unsupported) = predicates.partition {
        PredicateParser.compile(_).isDefined
      }
      supportedPredicates = Some(supported)
      unsupported
    } else {
      predicates
    }
  }

  override def pushedPredicates(): Array[Predicate] = supportedPredicates.getOrElse(Array.empty)
}