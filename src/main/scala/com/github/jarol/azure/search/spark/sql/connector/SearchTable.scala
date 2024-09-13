package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.config.{ReadConfig, WriteConfig}
import com.github.jarol.azure.search.spark.sql.connector.read.SearchScanBuilder
import com.github.jarol.azure.search.spark.sql.connector.write.SearchWriteBuilder
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class SearchTable(private val tableSchema: StructType)
  extends Table
    with SupportsRead
      with SupportsWrite {

  override def name(): String = "AzureSearchTable()"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {

    new util.HashSet[TableCapability]() {{
      add(TableCapability.BATCH_READ)
      add(TableCapability.BATCH_WRITE)
    }}
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {

    new SearchScanBuilder(
      schema(),
      ReadConfig(
        JavaScalaConverters.javaMapToScala(options)
      )
    )
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {

    new SearchWriteBuilder(
      WriteConfig(
        JavaScalaConverters.javaMapToScala(info.options())
      ),
      info.schema()
    )
  }
}
