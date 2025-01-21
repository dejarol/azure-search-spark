package io.github.jarol.azure.search.spark.sql.connector.read

import io.github.jarol.azure.search.spark.sql.connector.core.IndexDoesNotExistException
import io.github.jarol.azure.search.spark.sql.connector.read.config.ReadConfig
import io.github.jarol.azure.search.spark.sql.connector.read.filter.{ODataFilterExpression, ODataFilterExpressionBuilder}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.{StructField, StructType}

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
    with SupportsPushDownV2Filters
      with SupportsPushDownRequiredColumns {

  private var prunedSchema: StructType = schema
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
      val supportedODataExpressions: Seq[ODataFilterExpression] = supportedPredicates
        .map(ODataFilterExpressionBuilder.build)
        .collect {
          case Some(value) => value
        }

      new SearchScan(
        readConfig
          .withPushedPredicates(supportedODataExpressions)
          .withSelectClause(prunedSchema),
        prunedSchema
      )
    }
  }

  /**
   * Pushes down predicates, returning predicates to be evaluated after scanning
   * @param predicates predicate pushed down
   * @return predicates to be evaluated after scanning
   */

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {

    // The method should return predicates to be evaluated after scanning
    // So, if pushdown is enabled, we should separate supported predicates from unsupported
    if (readConfig.pushdownPredicate) {

      val (supported, unsupported) = predicates.partition {
        predicate => ODataFilterExpressionBuilder.build(predicate).isDefined
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

  /**
   * Applies column pruning with respect to a given required schema
   * @param requiredSchema required schema
   */

  override def pruneColumns(requiredSchema: StructType): Unit = {

    // Retain only the fields within required schema
    val requiredFields: Seq[StructField] = schema.filter {
      original =>
        requiredSchema.exists {
          required => original.name.equalsIgnoreCase(required.name)
        }
    }

    prunedSchema = StructType(requiredFields)
  }
}