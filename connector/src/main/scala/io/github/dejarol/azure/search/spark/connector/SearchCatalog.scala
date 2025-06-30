package io.github.dejarol.azure.search.spark.connector

import com.azure.search.documents.indexes.models.SearchIndex
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.schema.SchemaUtils
import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig
import io.github.dejarol.azure.search.spark.connector.write.config.WriteConfig
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Map => JMap}

/**
 * [[org.apache.spark.sql.connector.catalog.TableCatalog]] implementation for this dataSource.
 * As Azure Search doesn't have namespaces, this catalog implementation simply doesn't care about namespace
 * management and will handle identifiers considering only the table name.
 * In order to use this catalog, you need to define the following in your application's Spark configuration
 * {{{
 *  spark.sql.catalog.search_catalog = io.github.dejarol.azure.search.spark.connector.SearchCatalog
 *  spark.sql.catalog.search_catalog.endpoint = yourEndpoint
 *  spark.sql.catalog.search_catalog.adminKey = yourAdminKey
 * }}}
 * @since 0.11.0
 */

class SearchCatalog
  extends TableCatalog {

  import SearchCatalog._

  private var catalogName: Option[String] = None
  private var originalOptions: Option[CaseInsensitiveStringMap] = None

  /**
   * List all the tables in the catalog
   * @param namespace table namespace (unused as Azure Search doesn't have namespaces)
   * @return
   */

  override def listTables(namespace: Array[String]): Array[Identifier] = {

    // Retrieve the list of existing indexes
    // and then convert them to identifiers
    getReadConfig.listIndexes
      .map(identifierOf)
      .toArray
  }

  /**
   * Load a table's metadata, if the table exists
   * @param ident table identifier
   * @return table metadata
   */

  override def loadTable(ident: Identifier): Table = {

    val readConfig = getReadConfig.withIndexName(ident.name())
    val indexExists = readConfig.indexExists
    if (indexExists) {

      // Retrieve the index fields and convert them to a StructType
      val tableSchema = SchemaUtils.toStructType(
        readConfig.getSearchIndexFields
      )

      new SearchTable(tableSchema, ident.name(), readConfig)
    } else {
      throw new NoSuchTableException(ident)
    }
  }

  /**
   * Create a table. Table will be created if
   *  - it doesn't exist
   *  - it does not define partitions
   * @param ident table identifier
   * @param schema table schema
   * @param partitions table partitions (unsupported by this catalog)
   * @param properties table properties
   * @return metadata of newly created table
   */

  override def createTable(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: JMap[String, String]
                          ): Table = {

    // Enrich write config with table properties
    val indexName = ident.name()
    val writeConfig = getWriteConfig
      .withIndexName(indexName)
      .withOptions(properties)

    // If the index already exists, we have to throw a TableAlreadyExistsException
    if (writeConfig.indexExists) {
      throw new TableAlreadyExistsException(ident)
    } else {
      if (partitions.nonEmpty) {
        throw new UnsupportedOperationException(
          "Partitioning is not supported by this catalog"
        )
      }

      writeConfig.createIndex(indexName, schema)
      new SearchTable(schema, indexName, writeConfig)
    }
  }

  /**
   * Alter an existing table.
   * As changes to existing Azure Search indexes require reloading data, this operation is not supported
   * @param ident table identifier
   * @param changes changes to apply
   * @return metadata of the altered table
   */

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {

    val readConfig = getReadConfig.withIndexName(ident.name())
    if (readConfig.indexExists) {
      throw new IllegalArgumentException(
        s"Altering an Azure Search index is not supported"
      )
    } else {
      throw new NoSuchTableException(ident)
    }
  }

  /**
   * Drop a table from the catalog, if it exists
   * @param ident table identifier
   * @return true if the table was dropped
   */

  override def dropTable(ident: Identifier): Boolean = {

    val writeConfig = getWriteConfig.withIndexName(ident.name())
    if (writeConfig.indexExists) {
      writeConfig.deleteIndex(ident.name())
      true
    } else {
      // Otherwise, return false
      false
    }
  }

  /**
   * Rename a table. As Azure Search doesn't allow index renaming, this operation is not supported
   * @param oldIdent old identifier
   * @param newIdent new identifier
   */

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {

    val writeConfig = getWriteConfig
    val oldIdentDoesNotExist = !writeConfig.indexExists(oldIdent.name())

    // If the table referred by the old identifier doesn't exist, throw NoSuchTableException
    if (oldIdentDoesNotExist) {
      throw new NoSuchTableException(oldIdent)
    }

    // If the table referred by the new identifier already exists, throw TableAlreadyExistsException
    val newIdentExists = writeConfig.indexExists(newIdent.name())
    if (newIdentExists) {
      throw new TableAlreadyExistsException(newIdent)
    }

    throw new UnsupportedOperationException(
      "Renaming an Azure Search index is not supported"
    )
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {

    catalogName = Some(name)
    originalOptions = Some(options)
  }

  override def name(): String = {

    catalogName match {
      case Some(name) => name
      case None => throw new IllegalStateException(
        "Catalog name not initialized"
      )
    }
  }

  /**
   * Create a [[io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig]] instance from the original
   * options passed to the catalog during initialization
   * @return a read configuration instance
   */

  private def getReadConfig: ReadConfig = {

    ReadConfig(
      originalOptions.map {
        JavaScalaConverters.javaMapToScala
      }.getOrElse(Map.empty)
    )
  }

  /**
   * Create a [[io.github.dejarol.azure.search.spark.connector.write.config.WriteConfig]] instance from the original
   * options passed to the catalog during initialization
   * @return a write configuration instance
   */

  private def getWriteConfig: WriteConfig = {

    WriteConfig(
      originalOptions.map {
        JavaScalaConverters.javaMapToScala
      }.getOrElse(Map.empty)
    )
  }
}

object SearchCatalog {

  /**
   * Creates a catalog identifier from a name. As Azure Search doesn't have namespaces, the identifier's namespace
   * would be empty while the identifier's name would be the given name
   * @param name identifier name
   * @return an identifier for this datasource's catalog implementation
   */

  private[connector] def identifierOf(name: String): Identifier = {

    Identifier.of(
      Array.empty, name
    )
  }

  /**
   * Creates a catalog identifier for an index
   * @param index an existing Search index
   * @return an identifier for the index
   */

  private[connector] def identifierOf(index: SearchIndex): Identifier = identifierOf(index.getName)
}
