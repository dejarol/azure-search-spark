package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output.SearchPropertyConverter
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructField

class SearchBatchWrite(private val writeConfig: WriteConfig,
                       private val converters: Map[StructField, SearchPropertyConverter],
                       private val indexActionSupplier: IndexActionSupplier)
  extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {

    new SearchWriterFactory(
      writeConfig,
      converters,
      indexActionSupplier
    )
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}
