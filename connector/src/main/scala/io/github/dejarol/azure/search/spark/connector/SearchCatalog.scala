package io.github.dejarol.azure.search.spark.connector

import com.azure.search.documents.indexes.models.SearchIndex
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig
import io.github.dejarol.azure.search.spark.connector.write.config.WriteConfig
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

    println(s"Loading table $ident")
    // TODO: implement
    null
  }

  override def createTable(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: JMap[String, String]
                          ): Table = {

    // TODO: implement
    null
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {

    // TODO: implement
    null
  }

  override def dropTable(ident: Identifier): Boolean = {

    val writeConfig = getWriteConfig

    // Boolean conditions for allowing the table to be dropped
    val noNamespace = ident.namespace().isEmpty
    val identExists = writeConfig.indexExists(ident.name())

    // If the identifier has no namespace and the index exists, delete it
    if (noNamespace && identExists) {
      writeConfig.withSearchIndexClientDo {
        client => client.deleteIndex(
          ident.name()
        )
      }
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
   * Creates a catalog identifier for an index. As Azure Search doesn't have namespaces, the identifier's namespace
   * would be empty while the identifier's name would be the index name
   * @param index an existing Search index
   * @return an identifier for the index
   */

  private[connector] def identifierOf(index: SearchIndex): Identifier = {

    Identifier.of(
      Array.empty, index.getName
    )
  }
}
