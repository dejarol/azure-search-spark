package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import org.apache.spark.sql.connector.write.{Write, WriteBuilder}
import org.apache.spark.sql.types.StructType

/**
 * Write builder for Search dataSource
 * @param writeConfig write configuration
 * @param schema schema of input [[org.apache.spark.sql.DataFrame]] (retrieved by [[org.apache.spark.sql.connector.write.LogicalWriteInfo]])
 */

class SearchWriteBuilder(private val writeConfig: WriteConfig,
                         private val schema: StructType)
  extends WriteBuilder {

  override def build(): Write = {

    val targetIndexFields: Seq[SearchField] = if (writeConfig.indexExists) {
      writeConfig.getSearchIndexFields
    } else {
      SearchWriteBuilder.createIndex(
        writeConfig,
        schema
      ) match {
        case Left(value) => throw value
        case Right(value) => JavaScalaConverters.listToSeq(value.getFields)
      }
    }

    val indexActionTypeGetter: Option[IndexActionTypeGetter] = writeConfig.actionColumn.map {
      IndexActionTypeGetter(_, schema, writeConfig.overallAction)
    }

    new SearchWrite(
      writeConfig,
      schema,
      None,
      indexActionTypeGetter
    )
  }
}

object SearchWriteBuilder {

  def createIndex(
                   writeConfig: WriteConfig,
                   schema: StructType
                 ): Either[IndexCreationException, SearchIndex] = {

    Left(
      new IndexCreationException("a", null)
    )
    /*
    Try {
      val createIndexOptions = writeConfig.createIndexOptions
      writeConfig.withSearchIndexClientDo {
        _.createOrUpdateIndex(createIndexOptions.toSearchFields(schema))
      }
    }.toEither.left.map(
      new IndexCreationException(
        writeConfig.getIndex,
        _
      )
    )

     */
  }
}