package io.github.dejarol.azure.search.spark.connector.read

import io.github.dejarol.azure.search.spark.connector.core.NoSuchSearchIndexException
import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig
import io.github.dejarol.azure.search.spark.connector.read.filter.{ODataExpression, ODataExpressionV1FilterFactory}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Scan builder for Search DataSource
 *
 * @param readConfig read configuration
 * @param schema index schema (either inferred or defined by the user)
 */

class SearchScanBuilder(
                         private val readConfig: ReadConfig,
                         private val schema: StructType
                       )
  extends ScanBuilder
    with SupportsPushDownFilters
      with SupportsPushDownRequiredColumns {

  private var prunedSchema: StructType = schema
  private var supportedPredicates: Array[Filter] = Array.empty
  private lazy val predicateFactory = new ODataExpressionV1FilterFactory(schema)

  /**
   * Build the scan
 *
   * @throws NoSuchSearchIndexException if the target index does not exist
   * @return a scan to be used for Search DataSource
   */

  @throws[NoSuchSearchIndexException]
  override def build(): Scan = {

    if (!readConfig.indexExists) {
      throw new NoSuchSearchIndexException(readConfig.getIndex)
    } else {
      val supportedODataExpressions: Seq[ODataExpression] = supportedPredicates
        .map(predicateFactory.build)
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
   * @param filters predicate pushed down
   * @return predicates to be evaluated after scanning
   */

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {

    // The method should return predicates to be evaluated after scanning
    // So, if pushdown is enabled, we should separate supported predicates from unsupported
    if (readConfig.pushdownPredicate) {

      val (supported, unsupported) = filters.partition {
        predicate => predicateFactory.build(predicate).isDefined
      }

      supportedPredicates = supported
      unsupported
    } else {

      // If pushdown is disabled, return all predicates
      filters
    }
  }

  /**
   * Return the predicates that this datasource supports for pushdown
   * @return predicates that can be pushed
   */

  override def pushedFilters(): Array[Filter] = supportedPredicates

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