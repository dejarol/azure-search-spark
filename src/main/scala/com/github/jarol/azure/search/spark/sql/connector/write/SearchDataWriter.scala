package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.IndexDocumentsBatch
import com.azure.search.documents.models.{IndexAction, IndexActionType}
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class SearchDataWriter(private val writeConfig: WriteConfig,
                       private val schema: StructType)
  extends DataWriter[InternalRow] {

  private lazy val batchSize: Int = writeConfig.batchSize
  private lazy val actionTypeGetter: Option[IndexActionTypeGetter] = writeConfig.actionColumn
    .map {
      IndexActionTypeGetter(
      _, schema
    )
  }

  private var actionsBatch: Seq[IndexAction[SearchDocument]] = Seq.empty

  override def write(record: InternalRow): Unit = {

    // Retrieve action type from current record or use a default
    val actionType: IndexActionType = actionTypeGetter.map {
      getter => getter.apply(record)
    }.getOrElse(IndexActionType.MERGE_OR_UPLOAD)

    val document: SearchDocument = null
    val indexAction: IndexAction[SearchDocument] = new IndexAction[SearchDocument]
      .setDocument(document)
      .setActionType(actionType)

    actionsBatch = actionsBatch :+ indexAction
    if (actionsBatch.size.equals(batchSize)) {
      val documentsBatch = new IndexDocumentsBatch[SearchDocument]
        .addActions(
          JavaScalaConverters.seqToList(actionsBatch)
        )
    }
  }

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???

  override def close(): Unit = ???
}
