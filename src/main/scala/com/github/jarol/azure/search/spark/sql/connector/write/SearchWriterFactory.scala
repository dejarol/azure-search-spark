package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

class SearchWriterFactory(private val writeConfig: WriteConfig,
                          private val schema: StructType)
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {

    new SearchDataWriter(
      writeConfig,
      schema
    )
  }
}
