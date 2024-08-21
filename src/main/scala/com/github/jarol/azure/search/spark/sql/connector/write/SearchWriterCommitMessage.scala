package com.github.jarol.azure.search.spark.sql.connector.write

import org.apache.spark.sql.connector.write.WriterCommitMessage

case class SearchWriterCommitMessage(partitionId: Int,
                                     taskId: Long)
  extends WriterCommitMessage
