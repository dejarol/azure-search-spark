package com.github.jarol.azure.search.spark.sql.connector.write

import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.output.WriteConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructField

/**
 * [[DataWriterFactory]] implementation for Search dataSource
 *
 * @param writeConfig write configuration
 * @param converters mapping for converting a Spark internal row to a Search document
 * @param indexActionSupplier index action supplier
 */

class SearchWriterFactory(
                           private val writeConfig: WriteConfig,
                           private val converters: Map[StructField, WriteConverter],
                           private val indexActionSupplier: IndexActionSupplier
                         )
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {

    new SearchDataWriter(
      writeConfig,
      converters,
      indexActionSupplier,
      partitionId,
      taskId
    )
  }
}
