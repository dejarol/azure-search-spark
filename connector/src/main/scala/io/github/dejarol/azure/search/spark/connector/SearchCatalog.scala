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
 * [[org.apache.spark.sql.connector.catalog.TableCatalog]] implementation for this dataSource
 * @since 0.11.0
 */

class SearchCatalog
  extends TableCatalog {

  import SearchCatalog._

  private var catalogName: Option[String] = None
  private var originalOptions: Option[CaseInsensitiveStringMap] = None

  override def listTables(namespace: Array[String]): Array[Identifier] = {

    // Retrieve the list of existing indexes
    // and then convert them to identifiers
    getReadConfig.listIndexes
      .map(identifierOf)
      .toArray
  }

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

  override def createTable(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: JMap[String, String]
                          ): Table = {

    // TODO: enrich write config with table properties
    val writeConfig = getWriteConfig
      .withIndexName(ident.name())

    if (writeConfig.indexExists) {
      throw new TableAlreadyExistsException(ident)
    } else {
      if (partitions.nonEmpty) {
        throw new UnsupportedOperationException(
          "Partitioning is not supported by this catalog"
        )
      }

      writeConfig.createIndex(ident.name(), schema)
      new SearchTable(schema, ident.name(), writeConfig)
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {

    val readConfig = getReadConfig.withIndexName(ident.name())
    if (readConfig.indexExists) {
      // TODO: implement
      null
    } else {
      throw new NoSuchTableException(ident)
    }
  }

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

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {

    throw new UnsupportedOperationException(
      "Renaming tables is not supported by this catalog"
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
