package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchIndex
import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.types.StructType

import scala.util.Try

/**
 * Write for Search dataSource
 * @param writeConfig write configuration
 * @param schema schema of input [[org.apache.spark.sql.DataFrame]]
 */

class SearchWrite(private val writeConfig: WriteConfig,
                  private val schema: StructType,
                  private val createIndexOptions: Option[CreateSearchIndexOptions],
                  private val indexActionTypeGetter: Option[IndexActionTypeGetter])
  extends Write {

  override def toBatch: BatchWrite = {

    createIndexOptions.map {
      createIndex(_) match {
        case Left(value) => throw value
        case Right(_) => new SearchBatchWrite(writeConfig, schema, indexActionTypeGetter)
      }
    }.getOrElse(
      new SearchBatchWrite(writeConfig, schema, indexActionTypeGetter)
    )
  }

  private def createIndex(createIndexOptions: CreateSearchIndexOptions): Either[IndexCreationException, SearchIndex] = {

    Try {
      writeConfig.withSearchIndexClientDo {
        _.createOrUpdateIndex(createIndexOptions.toSearchIndex(schema))
      }
    }.toEither.left.map(
      new IndexCreationException(
        createIndexOptions.name,
        _
      )
    )
  }
}
