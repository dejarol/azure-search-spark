package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.WriteConverter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructField

/**
 * [[BatchWrite]] implementation for Search dataSource
 * @param writeConfig write configuration
 * @param converters converters for mapping a Spark internal row to a Search document
 * @param indexActionSupplier index action supplier
 */

class SearchBatchWrite(
                        private val writeConfig: WriteConfig,
                        private val converters: Map[StructField, WriteConverter],
                        private val indexActionSupplier: IndexActionSupplier
                      )
  extends BatchWrite
    with Logging {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {

    new SearchWriterFactory(
      writeConfig,
      converters,
      indexActionSupplier
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
