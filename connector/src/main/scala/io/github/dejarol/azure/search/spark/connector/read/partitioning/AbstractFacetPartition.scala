package io.github.dejarol.azure.search.spark.connector.read.partitioning

/**
 * Parent class for partitions created by a [[FacetedPartitioner]] by retrieving a set of values
 * from a Search field that is facetable and filterable
 * @param partitionId partition id
 * @param facetFieldName name of the field used for faceting values
 */

abstract class AbstractFacetPartition(
                                       override protected val partitionId: Int,
                                       protected val facetFieldName: String
                                     )
  extends AbstractSearchPartition(partitionId) {
}