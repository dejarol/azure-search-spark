package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.core.IndexDoesNotExistException
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

/**
 * Scan builder for Search DataSource
 * @param readConfig read configuration
 * @param schema index schema (either inferred or defined by the user)
 */

class SearchScanBuilder(private val readConfig: ReadConfig,
                        private val schema: StructType)
  extends ScanBuilder {

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
}