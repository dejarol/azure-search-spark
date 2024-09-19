package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.config.WriteConfig
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.output.SearchPropertyConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructField

class SearchWriterFactory(private val writeConfig: WriteConfig,
                          private val converters: Map[StructField, SearchPropertyConverter],
                          private val indexActionSupplier: IndexActionSupplier)
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {

    new SearchDataWriter(
      writeConfig,
      indexActionSupplier,
      converters,
      partitionId,
      taskId
    )
  }
}
