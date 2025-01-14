package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsBuilder

/**
 * Partition related to facet field value
 * <br>
 * Given a field named <b>f</b> and a value <b>v</b>, this partition will generate a facet filter which is
 * {{{
 *   f = v
 * }}}
 * to be then used together with the input filter (if defined)
 *
 * @param optionsBuilder delegate object for building the search options for this partition
 * @param facetFieldName name of the field used for faceting values
 * @param facetValue facet value for this partition
 */

case class FacetValuePartition(
                                override protected val partitionId: Int,
                                override protected val optionsBuilder: SearchOptionsBuilder,
                                override protected val facetFieldName: String,
                                protected val facetValue: String
                              )
  extends AbstractFacetPartition(partitionId, optionsBuilder, facetFieldName) {

  override protected[partitioning] def facetFilter: String = s"$facetFieldName eq $facetValue"
}