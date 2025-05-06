package io.github.dejarol.azure.search.spark.connector.read

import io.github.dejarol.azure.search.spark.connector.core.{Constants, JavaScalaConverters}
import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig
import io.github.dejarol.azure.search.spark.connector.read.partitioning.{SearchPartition, SearchPartitioner}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

import scala.util.Try

/**
 * [[org.apache.spark.sql.connector.read.Batch]] implementation for Search dataSource
 * @param readConfig read configuration
 * @param prunedSchema schema passed by the Scan implementation (pruned, if necessary)
 */

class SearchBatch(
                   private val readConfig: ReadConfig,
                   private val prunedSchema: StructType
                 )
  extends Batch
    with Logging {

  @throws[SearchBatchException]
  override def planInputPartitions(): Array[InputPartition] = {

    // Retrieve the partitioner instance and create the input partitions
    SearchBatch.createPartitioner(readConfig) match {
      case Left(value) => throw value
      case Right(value) => createPartitions(value)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = new SearchPartitionReaderFactory(readConfig, prunedSchema)

  /**
   * Create an array of partitions from a partitioner
   * @param partitioner partitioner instance
   * @throws SearchBatchException if some partitions exceed the number of retrieved documents
   * @return the created partitions
   */

  @throws[SearchBatchException]
  private def createPartitions(partitioner: SearchPartitioner): Array[InputPartition] = {

    val partitionsList: Seq[SearchPartition] = JavaScalaConverters.listToSeq(partitioner.createPartitions())
    log.info(s"Generated ${partitionsList.size} partition(s) using ${partitioner.getClass.getName}")

    // Filter the generated partitions
    val invalidPartitions = getInvalidPartitions(partitionsList)
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

  /**
   * Filter the given set of partitions, preserving only invalid ones
   * <br>
   * A partition is considered invalid if it's expected to retrieve more documents than the threshold set by
   * the service itself
   * @param partitions partitions
   * @return a list of invalid partitions
   */

  private def getInvalidPartitions(partitions: Seq[SearchPartition]): Seq[SearchPartition] = {

    // Filter partitions, preserving the ones whose document count exceeds service threshold
    partitions.filter {
      partition =>
        readConfig.getCountForPartition(partition) >
          Constants.DOCUMENTS_PER_PARTITION_LIMIT
    }
  }
}

object SearchBatch {

  /**
   * Try to create an instance of [[SearchPartitioner]]
   * @param readConfig read configuration
   * @return either a [[SearchBatchException]] or the created partitioner
   */

  private[read] def createPartitioner(readConfig: ReadConfig): Either[SearchBatchException, SearchPartitioner] = {

    // Try to detect a one-arg constructor requiring a ReadConfig
    // and create a partitioner instance
    Try {
      val partitionerFactory = readConfig.partitionerFactory
      partitionerFactory.createPartitioner(readConfig)
    }.toEither.left.map {
      SearchBatchException.forFailedPartitionerCreation
    }
  }
}