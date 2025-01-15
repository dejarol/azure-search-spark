package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.read.SearchScanBuilder
import com.github.jarol.azure.search.spark.sql.connector.read.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.write.{SearchWriteBuilder, WriteConfig}
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{HashSet => JHashSet, Set => JSet}

/**
 * [[Table]] implementation for Search dataSource
 * @param tableSchema table schema (either inferred or user-provided)
 * @param tableName table name
 */

class SearchTable(
                   private val tableSchema: StructType,
                   private val tableName: String
                 )
  extends Table
    with SupportsRead
      with SupportsWrite {

  override def name(): String = s"AzureSearchTable($tableName)"

  override def schema(): StructType = tableSchema

  override def capabilities(): JSet[TableCapability] = {

    new JHashSet[TableCapability]() {{
      add(TableCapability.BATCH_READ)
      add(TableCapability.BATCH_WRITE)
      add(TableCapability.TRUNCATE)
    }}
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {

    new SearchScanBuilder(
      ReadConfig(
        JavaScalaConverters.javaMapToScala(
          options
        )
      ),
      schema()
    )
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {

    new SearchWriteBuilder(
      WriteConfig(
        JavaScalaConverters.javaMapToScala(
          info.options()
        )
      ),
      info.schema()
    )
  }
}
