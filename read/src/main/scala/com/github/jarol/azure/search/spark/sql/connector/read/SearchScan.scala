package com.github.jarol.azure.search.spark.sql.connector.read

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

/**
 * Scan for Search dataSource
 * @param schema input schema (either inferred or user-defined)
 * @param readConfig read configuration
 */

class SearchScan(private val schema: StructType,
                 private val readConfig: ReadConfig)
  extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = new SearchBatch(readConfig, schema)
}