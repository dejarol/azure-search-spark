package io.github.dejarol.azure.search.spark.connector.read

import io.github.dejarol.azure.search.spark.connector.core.schema.conversion.SchemaViolationException
import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig
import io.github.dejarol.azure.search.spark.connector.read.partitioning.SearchPartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

/**
 * Partition reader factory for Search dataSource
 *
 * @param readConfig read configuration
 * @param prunedSchema schema passed by the Batch implementation of this source (pruned, if necessary)
 */

class SearchPartitionReaderFactory(
                                    private val readConfig: ReadConfig,
                                    private val prunedSchema: StructType
                                  )
  extends PartitionReaderFactory {

  /**
   * Create a Search partition reader
   * @param partition input partition instance
   * @throws UnexpectedPartitionTypeException if the input partition is not an instance of [[SearchPartition]]
   * @return a partition reader for Search dataSource
   */

  @throws[UnexpectedPartitionTypeException]
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {

    partition match {
      case sp: SearchPartition => createSearchReader(sp)
      case _ => throw new UnexpectedPartitionTypeException(partition.getClass)
    }
  }

  /**
   * Create the partition reader
   * @param partition instance of [[SearchPartition]]
   * @throws SchemaViolationException if the provided schema clashes with target index fields
   * @return a Search partition reader
   */

  @throws[SchemaViolationException]
  private def createSearchReader(partition: SearchPartition): PartitionReader[InternalRow] = {

    val searchDocumentToInternalRowConverter = SearchDocumentEncoder
      .safeApply(prunedSchema, readConfig.getSearchIndexFields) match {
      case Left(value) => throw value
      case Right(value) => value
    }

    new SearchPartitionReader(
      readConfig,
      searchDocumentToInternalRowConverter,
      partition
    )
  }
}
