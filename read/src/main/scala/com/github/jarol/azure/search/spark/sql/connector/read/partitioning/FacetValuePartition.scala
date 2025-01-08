package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.read.filter.ODataExpression

/**
 * Partition related to facet field value
 * <br>
 * Given a field named <b>f</b> and a value <b>v</b>, this partition will generate a facet filter which is
 * {{{
 *   f = v
 * }}}
 * to be then used together with the input filter (if defined)
 *
 * @param partitionId    partition id
 * @param inputFilter    optional filter to apply during data retrieval
 * @param maybeSelect    optional list of index fields to select
 * @param facetFieldName name of the field used for faceting values
 * @param facetValue facet value for this partition
 */

case class FacetValuePartition(
                                override protected val partitionId: Int,
                                override protected val inputFilter: Option[String],
                                override protected val maybeSelect: Option[Seq[String]],
                                override protected val pushedPredicates: Array[ODataExpression],
                                override protected val facetFieldName: String,
                                protected val facetValue: String
                              )
  extends AbstractFacetPartition(partitionId, inputFilter, maybeSelect, pushedPredicates, facetFieldName) {

  override def facetFilter: String = s"$facetFieldName eq $facetValue"
}