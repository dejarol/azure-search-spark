package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.ReadConverter
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

/**
 * Scan for Search dataSource
 * @param schema input schema (either inferred or user-defined)
 * @param readConfig read configuration
 * @param converters map with keys being field names and values being converters to use for extracting document values
 */

class SearchScan(private val schema: StructType,
                 private val readConfig: ReadConfig,
                 private val converters: Map[String, ReadConverter])
  extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = new SearchBatch(readConfig, converters)
}