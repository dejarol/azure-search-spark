package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.IndexDocumentsBatch
import com.azure.search.documents.models.{IndexAction, IndexActionType}
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class SearchDataWriter(private val writeConfig: WriteConfig,
                       private val schema: StructType,
                       private val partitionId: Int,
                       private val taskId: Long)
  extends DataWriter[InternalRow]
    with Logging {

  private lazy val internalRowToSearchDocumentConverter = InternalRowToSearchDocumentConverter(schema)
  private lazy val maybeActionGetter: Option[IndexActionTypeGetter] = writeConfig.actionColumn.map {
    IndexActionTypeGetter(_, schema, writeConfig.overallAction)
  }

  private var actionsBatch: Seq[IndexAction[SearchDocument]] = Seq.empty

  override def write(record: InternalRow): Unit = {

    // Retrieve action type from current record or use a default
    val actionType: IndexActionType = maybeActionGetter.map {
      getter => getter.apply(record)
    }.getOrElse(writeConfig.overallAction)

    // Create index action by setting document and action type
    val indexAction: IndexAction[SearchDocument] = new IndexAction[SearchDocument]
      .setDocument(internalRowToSearchDocumentConverter.apply(record))
      .setActionType(actionType)

    // Add the action to the batch
    actionsBatch = actionsBatch :+ indexAction

    // If the batch is full, write it
    if (actionsBatch.size >= writeConfig.batchSize) {
      writeDocuments()
    }
  }

  override def commit(): WriterCommitMessage = {

    writeDocuments()
    SearchWriterCommitMessage(partitionId, taskId)
  }

  override def abort(): Unit = {

    log.warn(s"Aborting writer for partition $partitionId")
  }

  override def close(): Unit = {

    log.info(s"Closing writer for partition $partitionId")
  }

  private def writeDocuments(): Unit = {

    if (actionsBatch.nonEmpty) {

      log.info(s"Starting to write ${actionsBatch.size} document(s) to index ${writeConfig.getIndex}. " +
        s"PartitionId: $partitionId, " +
        s"TaskId: $taskId"
      )
      val documentsBatch = new IndexDocumentsBatch[SearchDocument]
        .addActions(
          JavaScalaConverters.seqToList(actionsBatch)
        )

      writeConfig.withSearchClientDo {
        _.indexDocuments(documentsBatch)
      }

      actionsBatch = Seq.empty
    }
  }
}
