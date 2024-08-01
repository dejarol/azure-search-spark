package com.github.jarol.azure.search.spark.sql.connector.write

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}

class SearchDataWriter()
  extends DataWriter[InternalRow] {

  override def write(record: InternalRow): Unit = ???

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???

  override def close(): Unit = ???
}
