package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

import java.util

class SearchBatch(private val schema: StructType,
                  private val readConfig: ReadConfig)
  extends Batch
    with Logging {

  override def planInputPartitions(): Array[InputPartition] = {

    val partitioner = readConfig.partitioner
    val partitionsList: util.List[SearchPartition] = partitioner.generatePartitions()
    log.info(s"Generated ${partitionsList.size()} partition(s) using ${partitioner.getClass.getName}")

    partitioner
      .generatePartitions()
      .stream().toArray((value: Int) => Array.ofDim(value))
  }

  override def createReaderFactory(): PartitionReaderFactory = new SearchPartitionReaderFactory(schema, readConfig)
}
