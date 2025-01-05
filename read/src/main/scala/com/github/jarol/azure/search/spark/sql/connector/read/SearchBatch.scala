package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.core.{Constants, JavaScalaConverters}
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{SearchPartition, SearchPartitioner}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

/**
 * [[Batch]] implementation for Search dataSource
 * @param readConfig read configuration
 * @param schema schema
 */

class SearchBatch(
                   private val readConfig: ReadConfig,
                   private val schema: StructType,
                   private val pushedPredicates: Array[Predicate]
                 )
  extends Batch
    with Logging {

  @throws[SearchBatchException]
  override def planInputPartitions(): Array[InputPartition] = {

    // Retrieve the partitioner instance and create the input partitions
    val partitioner = SearchBatch.createPartitioner(readConfig, pushedPredicates)
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

  override def createReaderFactory(): PartitionReaderFactory = new SearchPartitionReaderFactory(readConfig, schema)

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
        readConfig.withSearchClientDo {
          partition.getCountPerPartition
        } > Constants.DOCUMENTS_PER_PARTITION_LIMIT
    }
  }
}

object SearchBatch {

  @throws[SearchBatchException]
  private[read] def createPartitioner(
                                       readConfig: ReadConfig,
                                       pushedPredicates: Array[Predicate]
                                     ): SearchPartitioner = {

    readConfig.partitionerClass.getDeclaredConstructor(
      classOf[ReadConfig],
      classOf[Array[Predicate]]
    ).newInstance(
      readConfig,
      pushedPredicates
    )
  }
}