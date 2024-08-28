package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.SparkInternalConverter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

import java.util

class SearchBatch(private val readConfig: ReadConfig,
                  private val converters: Map[String, SparkInternalConverter])
  extends Batch
    with Logging {

  override def planInputPartitions(): Array[InputPartition] = {

    val partitioner = readConfig.partitioner
    val partitionsList: util.List[SearchPartition] = partitioner.createPartitions()
    log.info(s"Generated ${partitionsList.size()} partition(s) using ${partitioner.getClass.getName}")

    partitioner
      .createPartitions()
      .stream().toArray((value: Int) => Array.ofDim(value))
  }

  override def createReaderFactory(): PartitionReaderFactory = new SearchPartitionReaderFactory(readConfig, converters)
}
