package io.github.jarol.azure.search.spark.sql.connector

import io.github.jarol.azure.search.spark.sql.connector.write.SearchWriteBuilder
import io.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import io.github.jarol.azure.search.spark.sql.connector.read.SearchScanBuilder
import io.github.jarol.azure.search.spark.sql.connector.read.config.ReadConfig
import io.github.jarol.azure.search.spark.sql.connector.write.config.WriteConfig
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

  /**
   * Gets the table name
   * @return table name
   */

  override def name(): String = s"AzureSearchTable($tableName)"

  /**
   * Gets the table schema
   * @return table schema
   */

  override def schema(): StructType = tableSchema

  /**
   * Gets the table capabilities
   * @return table capabilities
   */

  override def capabilities(): JSet[TableCapability] = {

    new JHashSet[TableCapability]() {{
      add(TableCapability.BATCH_READ)
      add(TableCapability.BATCH_WRITE)
      add(TableCapability.TRUNCATE)
    }}
  }

  /**
   * Creates the [[ScanBuilder]] implementation of this datasource
   * @param options set of options been configured at session level and/or provided to Spark reader
   * @return the [[ScanBuilder]] implementation of this datasource
   */

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

  /**
   * Creates the [[WriteBuilder]] implementation for this datasource
   * @param info logical write info
   * @return the [[WriteBuilder]] implementation for this datasource
   */

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
