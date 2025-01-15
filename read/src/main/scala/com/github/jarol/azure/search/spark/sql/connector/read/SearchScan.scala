package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.read.config.ReadConfig
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

/**
 * Scan for Search dataSource
 * @param readConfig read configuration
 * @param schema input schema (either inferred or user-defined)
 */

class SearchScan(
                  private val readConfig: ReadConfig,
                  private val schema: StructType
                )
  extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = new SearchBatch(readConfig, schema)

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