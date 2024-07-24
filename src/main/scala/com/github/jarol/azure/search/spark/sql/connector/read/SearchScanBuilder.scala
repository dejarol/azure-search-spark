package com.github.jarol.azure.search.spark.sql.connector.read

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

class SearchScanBuilder(private val inferredSchema: StructType)
  extends ScanBuilder {

  override def build(): Scan = new SearchScan(inferredSchema)
}
