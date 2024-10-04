package com.github.jarol.azure.search.spark.sql.connector.read

import org.apache.spark.sql.catalyst.analysis.NoSuchIndexException
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
   * @throws NoSuchIndexException if the target index does not exist
   * @return a scan to be used for Search DataSource
   */

  @throws[NoSuchIndexException]
  override def build(): Scan = {

    if (!readConfig.indexExists) {
      throw new NoSuchIndexException(readConfig.getIndex)
    } else {
      new SearchScan(schema, readConfig)
    }
  }
}