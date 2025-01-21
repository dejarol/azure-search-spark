package io.github.jarol.azure.search.spark.sql.connector

import io.github.jarol.azure.search.spark.sql.connector.core.{Constants, IndexDoesNotExistException, JavaScalaConverters}
import io.github.jarol.azure.search.spark.sql.connector.core.config.SearchIOConfig
import io.github.jarol.azure.search.spark.sql.connector.read.InferSchema
import io.github.jarol.azure.search.spark.sql.connector.read.config.ReadConfig
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Map => JMap}

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
   * @throws IndexDoesNotExistException if the target index does not exist
   * @return the index schema
   */

  @throws[IndexDoesNotExistException]
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {

    val readConfig = ReadConfig(
      JavaScalaConverters.javaMapToScala(options)
    )

    if (readConfig.indexExists) {
      InferSchema.forIndex(
        readConfig.getIndex,
        readConfig.getSearchIndexFields
      )
    } else {
      throw new IndexDoesNotExistException(readConfig.getIndex)
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
                         properties: JMap[String, String]
                       ): Table = {

    val config = new SearchIOConfig(
      JavaScalaConverters.javaMapToScala(properties)
    )

    new SearchTable(
      schema,
      config.getIndex
    )
  }

  /**
   * Returns the datasource short name
   * @return datasource's short name
   */

  override def shortName(): String = Constants.DATASOURCE_NAME

  /**
   * Returns true for data sources that accept a user-defined schema
   * @return true as this datasource allows external metadata
   */

  override def supportsExternalMetadata() = true

  /**
   * Returns the prefix that should be used for qualifying datasource options at session level.
   * <br>
   * For this datasource, each property prefixed with <b>spark.datasource.azure.search</b>
   * will be propagated to each read/write operation
   * @return prefix for datasource properties at session level
   */

  override def keyPrefix(): String = Constants.DATASOURCE_KEY_PREFIX
}