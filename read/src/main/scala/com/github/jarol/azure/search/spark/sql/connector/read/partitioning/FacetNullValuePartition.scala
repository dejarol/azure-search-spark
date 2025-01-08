package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.read.filter.ODataExpression

/**
 * A partition to use for retrieving all the documents that do not match a set of facet values covered by other partitions
 * (due to the facet field being either different or null)
 *
 * @param inputFilter    optional filter to apply during data retrieval
 * @param maybeSelect    optional list of index fields to select
 * @param pushedPredicates predicates that support pushdown
 * @param facetFieldName name of the field used for faceting values
 * @param facetValues facet values covered by other partitions
 */

case class FacetNullValuePartition(
                                    override protected val inputFilter: Option[String],
                                    override protected val maybeSelect: Option[Seq[String]],
                                    override protected val pushedPredicates: Array[ODataExpression],
                                    override protected val facetFieldName: String,
                                    protected val facetValues: Seq[String])
  extends AbstractFacetPartition(facetValues.size, inputFilter, maybeSelect, pushedPredicates, facetFieldName) {

  override def facetFilter: String = {

    val eqNull = s"$facetFieldName eq null"
    val equalToOtherValues = facetValues.map {
      value => s"$facetFieldName eq $value"
    }.mkString(" or ")

    s"$eqNull or not ($equalToOtherValues)"
  }
}