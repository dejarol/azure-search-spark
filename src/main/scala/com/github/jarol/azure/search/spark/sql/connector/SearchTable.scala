package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.read.SearchScanBuilder
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class SearchTable(private val inferredSchema: StructType)
  extends Table
    with SupportsRead
    with SupportsWrite {

  override def name(): String = f"AzureSearchTable(${"indexName"})"

  override def schema(): StructType = inferredSchema

  override def capabilities(): util.Set[TableCapability] = {

    ScalaToJava.set(
      Set(
        TableCapability.BATCH_READ,
        TableCapability.BATCH_WRITE
      )
    )
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new SearchScanBuilder(inferredSchema)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = ???
}
