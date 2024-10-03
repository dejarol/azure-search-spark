package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.config.SearchIOConfig
import com.github.jarol.azure.search.spark.sql.connector.core.{Constants, JavaScalaConverters, NoSuchIndexException}
import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/**
 * [[TableProvider]] implementation for Search dataSource
 */

class SearchTableProvider
  extends TableProvider
    with SessionConfigSupport
      with DataSourceRegister {

  /**
   * Infer the schema for a target Search index
   * @param options options for retrieving the Search index
   * @throws NoSuchIndexException if the target index does not exist
   * @return the index schema
   */

  @throws[NoSuchIndexException]
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {

    val readConfig = ReadConfig(
      JavaScalaConverters.javaMapToScala(options)
    )

    if (readConfig.indexExists) {
      InferSchema.forIndex(
        readConfig.getIndex,
        readConfig.getSearchIndexFields,
        readConfig.select
      )
    } else {
      throw new NoSuchIndexException(readConfig.getIndex)
    }
  }

  /**
   * Get the table for a Search index
   * @param schema table schema
   * @param partitioning partitioning
   * @param properties properties
   * @return a [[SearchTable]]
   */

  override def getTable(
                         schema: StructType,
                         partitioning: Array[Transform],
                         properties: util.Map[String, String]
                       ): Table = {

    val config = new SearchIOConfig(
      JavaScalaConverters.javaMapToScala(properties)
    )

    new SearchTable(
      schema,
      config.getIndex
    )
  }

  override def shortName(): String = Constants.DATASOURCE_NAME

  override def supportsExternalMetadata() = true

  override def keyPrefix(): String = Constants.DATASOURCE_NAME
}