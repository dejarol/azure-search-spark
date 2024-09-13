package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.input.SparkInternalConverter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

import java.util

/**
 * Batch for Search dataSource
 * @param readConfig read configuration
 * @param converters map with keys being document keys and values being converters from Search document properties to Spark internal objects
 */

class SearchBatch(private val readConfig: ReadConfig,
                  private val converters: Map[String, SparkInternalConverter])
  extends Batch
    with Logging {

  override def planInputPartitions(): Array[InputPartition] = {

    // Retrieve the partitioner instance and create the input partitions
    val partitioner = readConfig.partitioner
    val partitionsList: util.List[SearchPartition] = partitioner.createPartitions()
    log.info(s"Generated ${partitionsList.size()} partition(s) using ${partitioner.getClass.getName}")

    partitionsList
      .stream()
      .toArray((value: Int) => Array.ofDim(value))
  }

  override def createReaderFactory(): PartitionReaderFactory = new SearchPartitionReaderFactory(readConfig, converters)
}
