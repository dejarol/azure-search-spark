package com.github.jarol.azure.search.spark.sql.connector.write

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}

class SearchWriterFactory()
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {

    new SearchDataWriter()
  }
}
