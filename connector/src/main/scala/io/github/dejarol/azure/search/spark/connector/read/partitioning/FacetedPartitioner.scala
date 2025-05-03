package io.github.dejarol.azure.search.spark.connector.read.partitioning

import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters

import java.util.{List => JList}

/**
 * Partitioner implementation that exploits the facet values retrieved from the Search service.
 * <br>
 * Given a facetable and filterable field and a collection of <code>n</code> facet values,
 * it will generate <code>n + 1</code> partitions according to the following behavior:
 *  - <code>n</code> partitions will retrieve documents whose values for the facetable field are equal to one of the <code>n</code> facet values
 *  - one partition will retrieve documents whose values for the facetable field are null or do not meet one of the <code>n</code> facet values
 * @param fieldName name of facet field
 * @param facetValues facet values
 */

case class FacetedPartitioner(
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