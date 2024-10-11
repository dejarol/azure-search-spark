package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.{Write, WriteBuilder}
import org.apache.spark.sql.types.StructType

import scala.util.Try

/**
 * Write builder for Search dataSource
 * @param writeConfig write configuration
 * @param schema schema of input [[org.apache.spark.sql.DataFrame]] (retrieved by [[org.apache.spark.sql.connector.write.LogicalWriteInfo]])
 */

class SearchWriteBuilder(private val writeConfig: WriteConfig,
                         private val schema: StructType)
  extends WriteBuilder
    with Logging {

  /**
   * Build the Write for this dataSource
   * @throws IndexCreationException if target index does not exist but could not be created
   * @return the Write for this dataSource
   */

  @throws[IndexCreationException]
  override def build(): Write = {

    if (writeConfig.indexExists) {
      log.info(s"Index ${writeConfig.getIndex} already exists")
      writeConfig.getSearchIndexFields
    } else {

      // Try to create target index
      SearchWriteBuilder.createIndex(
        writeConfig,
        schema
      ) match {
        case Left(value) => throw value
        case Right(value) =>
          log.info(s"Successfully created index ${writeConfig.getIndex}")
          JavaScalaConverters.listToSeq(value.getFields)
      }
    }

    new SearchWrite(
      writeConfig,
      schema
    )
  }
}

object SearchWriteBuilder {

  /**
   * Create the target Search index
   * @param writeConfig write configuration
   * @param schema dataFrame schema
   * @return either an [[IndexCreationException]] with the handled exception, or a [[SearchIndex]] object
   *         representing the created index
   */

  def createIndex(
                   writeConfig: WriteConfig,
                   schema: StructType
                 ): Either[IndexCreationException, SearchIndex] = {

    // Try to create the index
    Try {
      val indexName = writeConfig.getIndex
      val searchFields: Seq[SearchField] = writeConfig
        .searchFieldOptions
        .schemaToSearchFields(schema)

      writeConfig.withSearchIndexClientDo {
        _.createOrUpdateIndex(
          new SearchIndex(indexName)
            .setFields(searchFields: _*)
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