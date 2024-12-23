package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.{SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions
import scala.util.Try

/**
 * [[WriteBuilder]] implementation for Search dataSource
 * @param writeConfig write configuration
 * @param schema schema of input [[org.apache.spark.sql.DataFrame]] (retrieved by [[org.apache.spark.sql.connector.write.LogicalWriteInfo]])
 * @param shouldTruncate whether we should truncate target Search index or not (true for truncating)
 */

class SearchWriteBuilder(
                          private val writeConfig: WriteConfig,
                          private val schema: StructType,
                          private val shouldTruncate: Boolean
                        )
  extends WriteBuilder
    with SupportsTruncate
      with Logging {

  /**
   * Constructor for non-truncating [[WriteBuilder]]
   * @param writeConfig write configuration
   * @param schema schema of input [[org.apache.spark.sql.DataFrame]]
   */

  def this(
            writeConfig: WriteConfig,
            schema: StructType
          ) = {

    this(writeConfig, schema, false)
  }

  /**
   * Build the Write for this dataSource
   * @throws IndexCreationException if target index does not exist but could not be created
   * @return the Write for this dataSource
   */

  @throws[IndexCreationException]
  override def build(): Write = {

    val indexName = writeConfig.getIndex
    if (writeConfig.indexExists) {
      if (shouldTruncate) {
        truncateIndex(indexName)
      }
    } else {
      log.info(s"Index $indexName does not exist. Creating it now")
      unsafelyCreateIndex(indexName)
    }

    new SearchWrite(writeConfig, schema)
  }

  /**
   * Create the [[WriteBuilder]] implementation of this dataSource that supports table truncation
   * @return the write builder that support truncation during write
   */

  override def truncate(): WriteBuilder = {

    new SearchWriteBuilder(writeConfig, schema, true)
  }

  /**
   * Truncate (i.e. drop and recreate) an existing Search index
   * @param indexName Search index name
   */

  private def truncateIndex(indexName: String): Unit = {

    // Truncate existing index
    writeConfig.withSearchIndexClientDo {
      sc =>
        sc.deleteIndex(indexName)
        Thread.sleep(5000)
        log.info(s"Successfully deleted index $indexName")
    }

    // Recreate the index with the new schema
    unsafelyCreateIndex(indexName)
  }

  /**
   * Unsafely create an index using this instance's write config and schema
   * @throws IndexCreationException if index could not be created
   */

  @throws[IndexCreationException]
  private def unsafelyCreateIndex(indexName: String): Unit = {

    SearchWriteBuilder.safelyCreateIndex(writeConfig, schema) match {
      case Left(value) => throw value
      case Right(_) => log.info(s"Successfully created index $indexName")
    }
  }
}

object SearchWriteBuilder {

  /**
   * Convert a Search index instance to a [[SearchIndexOperations]]
   * @param index Search index definition
   * @return an instance of [[SearchIndexOperations]]
   */

  private implicit def toIndexOperations(index: SearchIndex): SearchIndexOperations = new SearchIndexOperations(index)

  /**
   * Create the target Search index
   * @param writeConfig write configuration
   * @param schema dataFrame schema
   * @return either an [[IndexCreationException]] with the handled exception, or a [[SearchIndex]] object
   *         representing the created index
   */

  def safelyCreateIndex(
                         writeConfig: WriteConfig,
                         schema: StructType
                       ): Either[IndexCreationException, SearchIndex] = {

    // Try to create the index
    Try {
      val indexName = writeConfig.getIndex
      val searchFields: Seq[SearchField] = writeConfig
        .searchFieldCreationOptions
        .toSearchFields(schema)

      val searchIndexActions: Seq[SearchIndexAction] = writeConfig
        .searchIndexCreationOptions
        .searchIndexActions

      writeConfig.withSearchIndexClientDo {
        _.createIndex(
          new SearchIndex(indexName)
            .setFields(searchFields: _*)
            .applyActions(searchIndexActions: _*)
        )
      }
    }.toEither.left.map(
      // Map the left side to a proper exception
      new IndexCreationException(
        writeConfig.getIndex,
        _
      )
    )
  }
}