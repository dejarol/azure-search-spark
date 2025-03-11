package io.github.dejarol.azure.search.spark.connector

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.read.SearchScanBuilder
import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig
import io.github.dejarol.azure.search.spark.connector.write.SearchWriteBuilder
import io.github.dejarol.azure.search.spark.connector.write.config.WriteConfig
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{HashSet => JHashSet, Set => JSet}

/**
 * [[org.apache.spark.sql.connector.catalog.Table]] implementation for this dataSource
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
   * Creates the [[org.apache.spark.sql.connector.read.ScanBuilder]] implementation of this datasource
   * @param options set of options been configured at session level and/or provided to Spark reader
   * @return the [[org.apache.spark.sql.connector.read.ScanBuilder]] implementation of this datasource
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
   * Creates the [[org.apache.spark.sql.connector.write.WriteBuilder]] implementation of this datasource
   * @param info logical write info
   * @return the [[org.apache.spark.sql.connector.write.WriteBuilder]] implementation of this datasource
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
