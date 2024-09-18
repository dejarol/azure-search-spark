package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import com.github.jarol.azure.search.spark.sql.connector.schema.SchemaUtils
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.{StructField, StructType}

class SearchBatchWrite(private val writeConfig: WriteConfig,
                       private val schema: StructType,
                       private val indexActionTypeGetter: Option[IndexActionTypeGetter])
  extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {

    new SearchWriterFactory(
      writeConfig,
      indexActionTypeGetter
    )
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

object SearchBatchWrite {

  def safeApply(
                 schema: Seq[StructField],
                 searchFields: Seq[SearchField]
               ): Either[String, SearchBatchWrite] = {

    val schemaFieldsAndSearchFields = SchemaUtils.matchNamesakeFields(schema, searchFields)
    val nonCompatibleFields = schemaFieldsAndSearchFields.filterNot {
      case (k, v) => SchemaUtils.areCompatibleFields(k, v)
    }

    if (nonCompatibleFields.nonEmpty) {
      Left("a")
    } else {

      Left("b")
    }
  }
}
