package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.{SearchPartition, UnexpectedPartitionTypeException}
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.SparkInternalConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

/**
 * Partition reader factory for Search dataSource
 * @param readConfig read configuration
 * @param converters map with keys being document keys and values being converters from Search properties to Spark internal objects
 */

class SearchPartitionReaderFactory(private val readConfig: ReadConfig,
                                   private val converters: Map[String, SparkInternalConverter])
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
      case sp: SearchPartition => new SearchPartitionReader(readConfig, converters, sp)
      case _ => throw new UnexpectedPartitionTypeException(partition.getClass)
    }
  }
}
