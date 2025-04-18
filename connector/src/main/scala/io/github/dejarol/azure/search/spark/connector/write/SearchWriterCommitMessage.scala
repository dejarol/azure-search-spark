package io.github.dejarol.azure.search.spark.connector.write

import org.apache.spark.sql.connector.write.WriterCommitMessage

/**
 * [[org.apache.spark.sql.connector.write.WriterCommitMessage]] implementation for Search dataSource
 *
 * @param partitionId partition id
 * @param taskId task id
 */

case class SearchWriterCommitMessage(
                                      partitionId: Int,
                                      taskId: Long
                                    )
  extends WriterCommitMessage
