package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input.SparkMappingSupplier
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
   * @throws ScanBuilderException if the Scan cannot be built (see [[ScanBuilderException]]'s documentation)
   * @return a scan to be used for Search DataSource
   */

  @throws[ScanBuilderException]
  override def build(): Scan = {

    if (!readConfig.indexExists) {
      throw ScanBuilderException.causedByNonExistingIndex(readConfig.getIndex)
    } else {
      SparkMappingSupplier.getMapping(schema, readConfig.getSearchIndexFields, readConfig.getIndex) match {
        case Left(value) => throw new ScanBuilderException(value)
        case Right(value) => new SearchScan(schema, readConfig, value)
      }
    }
  }
}