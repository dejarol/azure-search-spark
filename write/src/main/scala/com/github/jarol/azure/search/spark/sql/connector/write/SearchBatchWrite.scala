package com.github.jarol.azure.search.spark.sql.connector.write

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
 * [[BatchWrite]] implementation for Search dataSource
 * @param writeConfig write configuration
 * @param schema DataFrame schema
 */

class SearchBatchWrite(
                        private val writeConfig: WriteConfig,
                        private val schema: StructType,

                      )
  extends BatchWrite
    with Logging {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {

    new SearchWriterFactory(
      writeConfig,
      schema
    )
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

    val taskDescription = messages.collect {
      case SearchWriterCommitMessage(partitionId, taskId) =>
        s"task $taskId on partition $partitionId"
    }.mkString(", ")

    log.info(s"Committing $taskDescription")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}
