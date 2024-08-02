package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.IndexDocumentsBatch
import com.azure.search.documents.models.IndexAction
import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class SearchDataWriter(private val writeConfig: WriteConfig,
                       private val schema: StructType)
  extends DataWriter[InternalRow] {

  private val documentsBatch: IndexDocumentsBatch[SearchDocument] = new IndexDocumentsBatch[SearchDocument]

  override def write(record: InternalRow): Unit = {

    val action: IndexAction[SearchDocument] = new IndexAction[SearchDocument]
  }

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???

  override def close(): Unit = ???
}
