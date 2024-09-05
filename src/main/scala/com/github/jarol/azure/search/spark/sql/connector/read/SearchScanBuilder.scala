package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.schema.SchemaCompatibilityException
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

/**
 * Scan builder for Search DataSource
 * @param schema index schema (either inferred or defined by the user)
 * @param readConfig read configuration
 */

class SearchScanBuilder(private val schema: StructType,
                        private val readConfig: ReadConfig)
  extends ScanBuilder {

  /**
   * Build the scan
   * @throws SchemaCompatibilityException if there's a schema incompatibility
   * @return a scan to be used for Search DataSource
   */

  @throws[SchemaCompatibilityException]
  override def build(): Scan = {

    SearchScan.safeApply(schema, readConfig.getSearchIndexFields, readConfig) match {
      case Left(value) => throw new SchemaCompatibilityException(value)
      case Right(value) => value
    }
  }
}