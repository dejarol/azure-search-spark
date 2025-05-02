package io.github.dejarol.azure.search.spark.connector.read.partitioning

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters

import java.util.{List => JList}

case class FacetedPartitionerV2(
                                 private val fieldName: String,
                                 private val facetValues: Seq[String]
                               )
  extends SearchPartitioner {

  override def createPartitions(): JList[SearchPartition] = {

    // Create as many partitions as the number of retrieved facet values
    val partitionsForFacetsValues: Seq[AbstractFacetPartition] = facetValues
      .zipWithIndex.map {
        case (value, partitionId) =>
          FacetValuePartition(partitionId, fieldName, value)
      }

    // Add another partition for either null values or other facet values
    val partitionForEitherNullOrOtherFacetValues = FacetNullValuePartition(
      fieldName,
      facetValues
    )

    JavaScalaConverters.seqToList(
      partitionsForFacetsValues :+
        partitionForEitherNullOrOtherFacetValues
    )
  }
}