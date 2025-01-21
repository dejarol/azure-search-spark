package io.github.jarol.azure.search.spark.sql.connector.read

import io.github.jarol.azure.search.spark.sql.connector.read.config.ReadConfig
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

/**
 * Scan for Search dataSource
 * @param readConfig read configuration
 * @param prunedSchema the schema for this scan, passed by the ScanBuilder implementation (pruned, if necessary)
 */

class SearchScan(
                  private val readConfig: ReadConfig,
                  private val prunedSchema: StructType
                )
  extends Scan {

  override def readSchema(): StructType = prunedSchema

  override def toBatch: Batch = new SearchBatch(readConfig, prunedSchema)

  protected[read] val pushedPredicate: Option[String] = readConfig.searchOptionsBuilderConfig.pushedPredicate

  /**
   * Get the scan description
   * @return can description
   */

  override def description(): String = {

    s"${this.getClass.getSimpleName}(" +
      s"${readConfig.getIndex}, " +
      s"pushedPredicates: $pushedPredicate" +
      s")"
  }
}