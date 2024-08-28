package com.github.jarol.azure.search.spark.sql.connector.read

import com.github.jarol.azure.search.spark.sql.connector.AzureSparkException
import com.github.jarol.azure.search.spark.sql.connector.config.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.partitioning.SearchPartition
import com.github.jarol.azure.search.spark.sql.connector.schema.conversion.SparkInternalConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class SearchPartitionReaderFactory(private val readConfig: ReadConfig,
                                   private val converters: Map[String, SparkInternalConverter])
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {

    partition match {
      case sp: SearchPartition => new SearchPartitionReader(readConfig, converters, sp)
      case _ => throw new AzureSparkException(s"Found a partition of type ${partition.getClass.getName}, " +
        s"expecting a ${classOf[SearchPartition].getName}")
    }
  }
}
