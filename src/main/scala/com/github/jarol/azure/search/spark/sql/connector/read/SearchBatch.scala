package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class SearchBatch(private val schema: StructType,
                  private val readConfig: ReadConfig)
  extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    ???
  }

  override def createReaderFactory(): PartitionReaderFactory = new SearchPartitionReaderFactory(schema, readConfig)
}
