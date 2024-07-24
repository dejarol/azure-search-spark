package com.github.jarol.azure.search.spark.sql.connector.read

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

class SearchScan(private val schema: StructType)
  extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = new SearchBatch()
}
