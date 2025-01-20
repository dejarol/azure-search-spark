package io.github.jarol.azure.search.spark.sql.connector.read.partitioning

/**
 * Partition related to facet field value
 * <br>
 * Given a field named <code>f</code> and a value <code>v</code>, this partition will generate a facet filter which is
 * {{{
 *   f = v
 * }}}
 * @param facetFieldName name of the field used for faceting values
 * @param facetValue facet value for this partition
 */

case class FacetValuePartition(
                                override protected val partitionId: Int,
                                override protected val facetFieldName: String,
                                protected val facetValue: String
                              )
  extends AbstractFacetPartition(partitionId, facetFieldName) {

  override def getPartitionFilter: String = s"$facetFieldName eq $facetValue"
}