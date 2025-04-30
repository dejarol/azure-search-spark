package io.github.dejarol.azure.search.spark.connector.read.partitioning

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters

import java.util.{List => JList}

/**
 * Partitioner for range queries
 * @param fieldName field to partition by
 * @param partitionBounds list of partition bounds (for generating partitions)
 */

case class RangePartitioner(
                             private val fieldName: String,
                             private val partitionBounds: Seq[String]
                           )
  extends SearchPartitioner {

  override def createPartitions(): JList[SearchPartition] = {

    val lowerValues: Seq[Option[String]] = None +: partitionBounds.map(Some(_))
    val upperValues: Seq[Option[String]] = partitionBounds.map(Some(_)) :+ None

    // Zip lower values with upper values and create the partitions
    val partitionSeq = lowerValues.zip(upperValues).zipWithIndex.map {
      case ((lb, ub), index) =>
        RangePartition(index, fieldName, lb, ub)
    }

    JavaScalaConverters.seqToList(
      partitionSeq
    )
  }
}
