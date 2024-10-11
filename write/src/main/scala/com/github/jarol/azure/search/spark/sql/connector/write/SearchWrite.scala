package com.github.jarol.azure.search.spark.sql.connector.write

import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.types.StructType

/**
 * [[Write]] implementation for Search dataSource
 * @param writeConfig write configuration
 * @param schema schema of input [[org.apache.spark.sql.DataFrame]]
 */

class SearchWrite(
                   private val writeConfig: WriteConfig,
                   private val schema: StructType
                 )
  extends Write {

  override def toBatch: BatchWrite = new SearchBatchWrite(writeConfig, schema)
}
