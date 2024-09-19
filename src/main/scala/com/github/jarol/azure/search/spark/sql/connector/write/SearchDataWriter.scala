package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.IndexDocumentsBatch
import com.azure.search.documents.models.IndexAction
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output.SearchPropertyConverter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructField

class SearchDataWriter(private val writeConfig: WriteConfig,
                       private val actionSupplier: IndexActionSupplier,
                       private val converters: Map[StructField, SearchPropertyConverter],
                       private val partitionId: Int,
                       private val taskId: Long)
  extends DataWriter[InternalRow]
    with Logging {

  private lazy val internalRowToSearchDocumentConverter = InternalRowToSearchDocumentConverter(converters)

  private var actionsBuffer: Seq[IndexAction[SearchDocument]] = Seq.empty

  override def write(record: InternalRow): Unit = {

    // Retrieve action type from current record or use a default
    // Create index action by setting document and action type
    val indexAction: IndexAction[SearchDocument] = new IndexAction[SearchDocument]
      .setDocument(internalRowToSearchDocumentConverter.apply(record))
      .setActionType(actionSupplier.get(record))

    // Add the action to the batch
    actionsBuffer = actionsBuffer :+ indexAction

    // If the batch is full, write it
    if (actionsBuffer.size >= writeConfig.batchSize) {
      writeDocuments()
    }
  }

  override def commit(): WriterCommitMessage = {

    writeDocuments()
    SearchWriterCommitMessage(partitionId, taskId)
  }

  override def abort(): Unit = {

    log.warn(s"Aborting writing task $taskId on partition $partitionId")
  }

  override def close(): Unit = {

    log.info(s"Closing writer task $taskId on partition $partitionId")
  }

  private def writeDocuments(): Unit = {

    if (actionsBuffer.nonEmpty) {

      log.debug(s"Starting to write ${actionsBuffer.size} document(s) to index ${writeConfig.getIndex} " +
        s"(partitionId: $partitionId, taskId: $taskId)"
      )

      // Index documents
      writeConfig.indexDocuments(
        new IndexDocumentsBatch[SearchDocument]
          .addActions(
            JavaScalaConverters.seqToList(actionsBuffer)
          )
      )

      // Clear the actions buffer
      actionsBuffer = Seq.empty
    }
  }
}
