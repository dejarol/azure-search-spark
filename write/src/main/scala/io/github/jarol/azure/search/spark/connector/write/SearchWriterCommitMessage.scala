package io.github.jarol.azure.search.spark.connector.write

import org.apache.spark.sql.connector.write.WriterCommitMessage

/**
 * [[WriterCommitMessage]] implementation for Search dataSource
 *
 * @param partitionId partition id
 * @param taskId task id
 */

case class SearchWriterCommitMessage(partitionId: Int,
                                     taskId: Long)
  extends WriterCommitMessage
