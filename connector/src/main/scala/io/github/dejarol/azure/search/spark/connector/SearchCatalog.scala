package io.github.dejarol.azure.search.spark.connector

import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Map => JMap}

class SearchCatalog
  extends TableCatalog {

  private var catalogName: Option[String] = None
  private var originalOptions: Option[CaseInsensitiveStringMap] = None

  override def listTables(namespace: Array[String]): Array[Identifier] = {

    // TODO: implement
    Array.empty
  }

  override def loadTable(ident: Identifier): Table = {

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

    // TODO: implement
    false
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {

    // TODO: implement
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
}
