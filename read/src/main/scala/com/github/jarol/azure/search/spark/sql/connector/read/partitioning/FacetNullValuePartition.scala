package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsSupplier

/**
 * A partition to use for retrieving all the documents that do not match a set of facet values covered by other partitions
 * (due to the facet field being either different or null)
 *
 * @param optionsSupplier delegate object for getting the search options for this partition
 * @param facetFieldName name of the field used for faceting values
 * @param facetValues facet values covered by other partitions
 */

case class FacetNullValuePartition(
                                    override protected val optionsSupplier: SearchOptionsSupplier,
                                    override protected val facetFieldName: String,
                                    protected val facetValues: Seq[String]
                                  )
  extends AbstractFacetPartition(facetValues.size, optionsSupplier, facetFieldName) {

  override protected[partitioning] def facetFilter: String = {

    val eqNull = s"$facetFieldName eq null"
    val equalToOtherValues = facetValues.map {
      value => s"$facetFieldName eq $value"
    }.mkString(" or ")

    s"$eqNull or not ($equalToOtherValues)"
  }
}