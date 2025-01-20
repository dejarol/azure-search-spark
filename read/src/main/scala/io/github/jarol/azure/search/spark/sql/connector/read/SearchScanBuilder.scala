package io.github.jarol.azure.search.spark.sql.connector.read

import io.github.jarol.azure.search.spark.sql.connector.core.IndexDoesNotExistException
import io.github.jarol.azure.search.spark.sql.connector.read.config.ReadConfig
import io.github.jarol.azure.search.spark.sql.connector.read.filter.{ODataExpression, ODataExpressionBuilder}
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

  private var supportedPredicates: Array[Predicate] = Array.empty

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
      val supportedODataExpressions: Seq[ODataExpression] = supportedPredicates
        .map(ODataExpressionBuilder.build)
        .collect {
          case Some(value) => value
        }

      new SearchScan(
        readConfig.withPushedPredicates(supportedODataExpressions),
        schema
      )
    }
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {

    // The method should return predicates to be evaluated after scanning
    // So, if pushdown is enabled, we should separate supported predicates from unsupported
    if (readConfig.pushdownPredicate) {

      val (supported, unsupported) = predicates.partition {
        predicate => ODataExpressionBuilder.build(predicate).isDefined
      }

      supportedPredicates = supported
      unsupported
    } else {

      // If pushdown is disabled, return all predicates
      predicates
    }
  }

  /**
   * Return the predicates that this datasource supports for pushdown
   * @return predicates that can be pushed
   */

  override def pushedPredicates(): Array[Predicate] = supportedPredicates
}