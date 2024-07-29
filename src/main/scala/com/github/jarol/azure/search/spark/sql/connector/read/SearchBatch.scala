package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class SearchBatch(private val schema: StructType,
                  private val readConfig: ReadConfig)
  extends Batch
    with Logging {

  override def planInputPartitions(): Array[InputPartition] = {

    val partitioner = readConfig.partitioner
    log.info(s"Generating partitions using ${partitioner.getClass.getName}")

    partitioner
      .generatePartitions()
      .stream().toArray((value: Int) => Array.ofDim(value))
  }

  override def createReaderFactory(): PartitionReaderFactory = new SearchPartitionReaderFactory(schema, readConfig)
}
