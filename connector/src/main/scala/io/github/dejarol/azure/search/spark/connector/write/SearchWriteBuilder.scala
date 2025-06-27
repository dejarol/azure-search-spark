package io.github.dejarol.azure.search.spark.connector.write

import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import io.github.dejarol.azure.search.spark.connector.write.config.WriteConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.types.StructType

import scala.util.Try

/**
 * [[org.apache.spark.sql.connector.write.WriteBuilder]] implementation for this dataSource
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
   * Constructor for non-truncating [[org.apache.spark.sql.connector.write.WriteBuilder]]
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
  override def buildForBatch(): BatchWrite = {

    val indexName = writeConfig.getIndex
    if (writeConfig.indexExists) {
      if (shouldTruncate) {
        truncateIndex(indexName)
      }
    } else {
      log.info(s"Index $indexName does not exist. Creating it now")
      unsafelyCreateIndex(indexName)
    }

    new SearchBatchWrite(writeConfig, schema)
  }

  /**
   * Create the [[org.apache.spark.sql.connector.write.WriteBuilder]] implementation of this dataSource that supports table truncation
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
    writeConfig.deleteIndex(indexName)
    Thread.sleep(1000)
    log.info(s"Successfully deleted index $indexName")

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
   * Create the target Search index
   * @param writeConfig write configuration
   * @param schema dataFrame schema
   * @return either an [[io.github.dejarol.azure.search.spark.connector.write.IndexCreationException]] with the handled exception,
   *         or a [[com.azure.search.documents.indexes.models.SearchIndex]] object representing the created index
   */

  def safelyCreateIndex(
                         writeConfig: WriteConfig,
                         schema: StructType
                       ): Either[IndexCreationException, SearchIndex] = {

    // Try to create the index
    Try {
      writeConfig.createIndex(
        writeConfig.getIndex,
        schema
      )
    }.toEither.left.map(
      // Map the left side to a proper exception
      new IndexCreationException(
        writeConfig.getIndex,
        _
      )
    )
  }
}