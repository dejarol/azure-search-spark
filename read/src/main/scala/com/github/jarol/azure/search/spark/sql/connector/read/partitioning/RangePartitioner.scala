package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.{DataTypeException, JavaScalaConverters}
import com.github.jarol.azure.search.spark.sql.connector.core.config.{ConfigException, SearchConfig}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.toSearchTypeOperations
import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig

import java.util

case class RangePartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  override def createPartitions(): util.List[SearchPartition] = {

    val partitionerOptions = readConfig.partitionerOptions
    val partitionFieldName = partitionerOptions.unsafelyGet(ReadConfig.PARTITION_FIELD_CONFIG)
    RangePartitioner.maybePartitionFieldType(
      readConfig.getSearchIndexFields,
      partitionFieldName
    ) match {
      case Left(value) => throw new ConfigException(ReadConfig.PARTITION_FIELD_CONFIG, partitionFieldName, value)
      case Right(value) =>
        JavaScalaConverters.seqToList(
          rangePartitions(value, partitionerOptions)
        )
    }
  }

  private def rangePartitions(
                               searchFieldDataType: SearchFieldDataType,
                               partitionerOptions: SearchConfig
                             ): Seq[SearchPartition] = {

    val lowerBound = partitionerOptions.unsafelyGet(ReadConfig.LOWER_BOUND_CONFIG)
    val upperBound = partitionerOptions.unsafelyGet(ReadConfig.UPPER_BOUND_CONFIG)
    Seq.empty
  }
}

object RangePartitioner {

  def maybePartitionFieldType(
                               searchFields: Seq[SearchField],
                               name: String
                             ): Either[Throwable, SearchFieldDataType] = {

    searchFields.collectFirst {
      case sf if sf.getName.equalsIgnoreCase(name) => sf.getType
    }.toRight().left.map {
      _ => new NoSuchSearchFieldException(name)
    }.right.flatMap {
      tp =>
        if (tp.isNumeric || tp.isDateTime) {
          Right(tp)
        } else {
          Left(
            new DataTypeException(
              s"Unsupported partition field type ($tp). Only numeric or datetime types are supported"
            )
          )
        }
    }
  }

  private def generateRangeBounds[T](
                                      lowerBound: String,
                                      upperBound: String,
                                      numPartitions: Int
                                    ): Seq[(Option[T], Option[T])] = {


    Seq.empty
  }
}
