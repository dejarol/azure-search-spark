package com.github.jarol.azure.search.spark.sql.connector.read

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.clients.ClientFactory
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

/**
 * Scan builder for Search DataSource
 * @param schema index schema (either inferred or defined by the user)
 * @param readConfig read configuration
 */

class SearchScanBuilder(private val schema: StructType,
                        private val readConfig: ReadConfig)
  extends ScanBuilder {

  /**
   * Build the scan
   * @throws SchemaCompatibilityException if there's a schema incompatibility
   * @return a scan to be used for Search DataSource
   */

  @throws[SchemaCompatibilityException]
  override def build(): Scan = {

    val index: String = readConfig.getIndex
    val searchFields: Seq[SearchField] = JavaScalaConverters.listToSeq(
      ClientFactory.searchIndex(readConfig).getFields
    )

    // Execute a schema compatibility check
    SchemaCompatibilityValidator.computeMismatches(
      schema,
      searchFields
    ) match {
      case Some(value) => throw new SchemaCompatibilityException(value.mkString(", "))
      case None => SearchScan(schema, searchFields, readConfig)
    }
  }
}