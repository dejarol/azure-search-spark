package com.github.jarol.azure.search.spark.sql.connector

import com.github.jarol.azure.search.spark.sql.connector.core.{NoSuchIndexException, JavaScalaConverters}
import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
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

  override def getTable(
                         schema: StructType,
                         partitioning: Array[Transform],
                         properties: util.Map[String, String]
                       ): Table = {

    new SearchTable(schema)
  }

  override def shortName(): String = SearchTableProvider.SHORT_NAME

  override def supportsExternalMetadata(): Boolean = true

}

object SearchTableProvider {

  /**
   * Datasource format
   */

  final val SHORT_NAME: String = "azuresearch"
}