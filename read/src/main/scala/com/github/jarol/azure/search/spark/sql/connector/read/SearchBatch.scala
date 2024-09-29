package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.core.{Constants, JavaScalaConverters}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.conversion.input.ReadConverter
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

/**
 * Batch for Search dataSource
 * @param readConfig read configuration
 * @param converters map with keys being document keys and values being converters from Search document properties to Spark internal objects
 */

class SearchBatch(private val readConfig: ReadConfig,
                  private val converters: Map[String, ReadConverter])
  extends Batch
    with Logging {

  override def planInputPartitions(): Array[InputPartition] = {

    // Retrieve the partitioner instance and create the input partitions
    val partitioner = readConfig.partitioner
    val partitionsList: Seq[SearchPartition] = JavaScalaConverters.listToSeq(partitioner.createPartitions())
    log.info(s"Generated ${partitionsList.size} partition(s) using ${partitioner.getClass.getName}")

    // Filter the generated partitions
    val invalidPartitions = partitionsList.filter {
      partition => readConfig.withSearchClientDo {
        partition.getCountPerPartition
      } > Constants.PARTITION_DOCUMENT_LIMIT
    }

    if (invalidPartitions.nonEmpty) {
      throw SearchBatchException.forInvalidPartitions(
        JavaScalaConverters.seqToList(
          invalidPartitions
        )
      )
    } else {
      partitionsList.toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = new SearchPartitionReaderFactory(readConfig, converters)
}
