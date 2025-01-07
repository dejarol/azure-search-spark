package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.core.{Constants, JavaScalaConverters}
import com.github.jarol.azure.search.spark.sql.connector.read.filter.V2ExpressionAdapter
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{SearchPartition, SearchPartitioner}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

import scala.util.Try

/**
 * [[Batch]] implementation for Search dataSource
 * @param readConfig read configuration
 * @param schema schema
 */

class SearchBatch(
                   private val readConfig: ReadConfig,
                   private val schema: StructType,
                   private val pushedPredicates: Array[V2ExpressionAdapter]
                 )
  extends Batch
    with Logging {

  @throws[SearchBatchException]
  override def planInputPartitions(): Array[InputPartition] = {

    // Retrieve the partitioner instance and create the input partitions
    SearchBatch.createPartitioner(readConfig, pushedPredicates) match {
      case Left(value) => throw value
      case Right(value) => createPartitions(value)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = new SearchPartitionReaderFactory(readConfig, schema)

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
        readConfig.withSearchClientDo {
          partition.getCountPerPartition
        } > Constants.DOCUMENTS_PER_PARTITION_LIMIT
    }
  }
}

object SearchBatch {

  /**
   * Try to create an instance of [[SearchPartitioner]]
   * @param readConfig read configuration
   * @param pushedPredicates predicates that can be pushed down to partitions
   * @return either a [[SearchBatchException]] or the created partitioner
   */

  private[read] def createPartitioner(
                                       readConfig: ReadConfig,
                                       pushedPredicates: Array[V2ExpressionAdapter]
                                     ): Either[SearchBatchException, SearchPartitioner] = {

    Try {
      readConfig.partitionerClass.getDeclaredConstructor(
        classOf[ReadConfig],
        classOf[Array[Predicate]]
      ).newInstance(
        readConfig,
        pushedPredicates
      )
    }.toEither.left.map {
      SearchBatchException.forFailedPartitionerCreation(
        readConfig.partitionerClass,
        _
      )
    }
  }
}