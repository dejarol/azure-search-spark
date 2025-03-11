package io.github.dejarol.azure.search.spark.connector.write

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.indexes.models.IndexDocumentsBatch
import com.azure.search.documents.models.IndexAction
import io.github.dejarol.azure.search.spark.connector.write.config.WriteConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}

/**
 * [[DataWriter]] implementation for Search dataSource
 * @param writeConfig write configuration
 * @param documentDecoder decoder from Spark internal rows to Search documents
 * @param actionSupplier index action supplier
 * @param partitionId partition id
 * @param taskId task id
 */

class SearchDataWriter(
                        private val writeConfig: WriteConfig,
                        private val documentDecoder: SearchDocumentDecoder,
                        private val actionSupplier: IndexActionSupplier,
                        private val partitionId: Int,
                        private val taskId: Long
                      )
  extends DataWriter[InternalRow]
    with Logging {

  private var actionsBuffer: Seq[IndexAction[SearchDocument]] = Seq.empty

  override def write(record: InternalRow): Unit = {

    // Create index action by setting document and action type
    val indexAction: IndexAction[SearchDocument] = new IndexAction[SearchDocument]
      .setDocument(documentDecoder.apply(record))
      .setActionType(actionSupplier.get(record))

    // Add the action to the batch and, if full, write it
    actionsBuffer = actionsBuffer :+ indexAction
    if (actionsBuffer.size >= writeConfig.batchSize) {
      writeDocuments()
    }
  }

  /**
   * Write remaining documents
   * @return a [[WriterCommitMessage]] for this writer instance
   */

  override def commit(): WriterCommitMessage = {

    writeDocuments()
    SearchWriterCommitMessage(partitionId, taskId)
  }

  override def abort(): Unit = {

    log.warn(s"Aborting writing task $taskId on partition $partitionId")
  }

  override def close(): Unit = log.info(s"Closing writing task $taskId on partition $partitionId")

  /**
   * Write documents to target Search index
   */

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
