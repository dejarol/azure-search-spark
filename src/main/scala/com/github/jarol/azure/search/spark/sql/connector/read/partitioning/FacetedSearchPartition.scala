package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchField

case class FacetedSearchPartition(override val partitionId: Int,
                                  override val maybeFilter: Option[String],
                                  override val maybeSelect: Option[Seq[String]],
                                  facetFilter: String)
  extends AbstractSearchPartition(partitionId, maybeFilter, maybeSelect) {

  override def getFilter: String = {

    // Combine facet filter with overall filter (if any)
    maybeFilter.map {
      f => s"$facetFilter and $f"
    }.getOrElse(facetFilter)
  }
}

object FacetedSearchPartition {

  /**
   * Generate a set of partitions exploiting values of a facetable field
   * @param maybeFilter optional overall filter
   * @param maybeSelect optional list of selection fields
   * @param facetField facet field
   * @param facets facet field values
   * @return a collection of Search partitions
   */

  def createCollection(maybeFilter: Option[String],
                       maybeSelect: Option[Seq[String]],
                       facetField: SearchField,
                       facets: Seq[Any]): Seq[FacetedSearchPartition] = {

    val valueFormatter = FilterValueFormatters.forType(facetField.getType)
    val facetFieldName: String = facetField.getName
    val facetFormattedValues: Seq[String] = facets.map {
      valueFormatter.format
    }

    val facetPartitionFilters: Seq[String] = facetFormattedValues.map {
      value => s"$facetFieldName eq $value"
    }

    val nullOrNotInOtherFacets: String = s"$facetFieldName eq null or " +
      s"not (${facetPartitionFilters.mkString(" or ")})"

    (facetPartitionFilters :+ nullOrNotInOtherFacets)
      .zipWithIndex.map {
        case (facetFilter, partitionId) =>
          FacetedSearchPartition(
            partitionId,
            maybeFilter,
            maybeSelect,
            facetFilter
          )
      }
  }
}
