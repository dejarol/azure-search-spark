package io.github.dejarol.azure.search.spark.connector.read.partitioning

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters

import java.util.{List => JList}

case class FacetedPartitionerV2(
                                 private val facetFieldName: String,
                                 private val facetStringValues: Seq[String]
                               )
  extends SearchPartitioner {

  override def createPartitions(): JList[SearchPartition] = {

    // Create as many partitions as the number of retrieved facet values
    val partitionsForFacetsValues: Seq[AbstractFacetPartition] = facetStringValues
      .zipWithIndex.map {
        case (value, partitionId) =>
          FacetValuePartition(partitionId, facetFieldName, value)
      }

    // Add another partition for either null values or other facet values
    val partitionForEitherNullOrOtherFacetValues = FacetNullValuePartition(
      facetFieldName,
      facetStringValues
    )

    JavaScalaConverters.seqToList(
      partitionsForFacetsValues :+ partitionForEitherNullOrOtherFacetValues
    )
  }
}