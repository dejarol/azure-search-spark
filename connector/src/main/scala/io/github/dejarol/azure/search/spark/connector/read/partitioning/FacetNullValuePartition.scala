package io.github.dejarol.azure.search.spark.connector.read.partitioning

/**
 * A partition to use for retrieving all the documents that do not match a set of facet values covered by other partitions
 * (due to the facet field being either different or null)
 * @param facetFieldName name of the field used for faceting values
 * @param facetValues facet values covered by other partitions
 */

case class FacetNullValuePartition(
                                    override protected val facetFieldName: String,
                                    protected val facetValues: Seq[String]
                                  )
  extends AbstractFacetPartition(facetValues.size, facetFieldName) {

  override def getPartitionFilter: String = {

    val eqNull = s"$facetFieldName eq null"
    val equalToOtherValues = facetValues.map {
      value => s"$facetFieldName eq $value"
    }.mkString(" or ")

    s"$eqNull or not ($equalToOtherValues)"
  }
}