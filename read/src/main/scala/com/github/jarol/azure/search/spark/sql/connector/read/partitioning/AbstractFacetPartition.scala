package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchField

/**
 * Parent class for partitions created by a [[FacetedPartitioner]] by retrieving a set of values
 * from a Search field that is facetable and filterable.
 * <br>
 * Each instance will retrieve documents that match the <b>facet filter</b> and the input filter, when defined
 * <br>
 * I.e. the predicate for matching documents will be
 * {{{
 *   inputFilter match {
 *    case Some(x) => facet filter and x
 *    case None => facet filter
 *   }
 * }}}
 * @param partitionId partition id
 * @param inputFilter optional filter to apply during data retrieval
 * @param maybeSelect optional list of index fields to select
 * @param facetFieldName name of the field used for faceting values
 */

abstract class AbstractFacetPartition(override protected val partitionId: Int,
                                      override protected val inputFilter: Option[String],
                                      override protected val maybeSelect: Option[Seq[String]],
                                      protected val facetFieldName: String)
  extends SearchPartitionTemplate(partitionId, inputFilter, maybeSelect) {

  override final def getSearchFilter: String = {

    inputFilter match {
      case Some(value) => s"$value and $facetFilter"
      case None => facetFilter
    }
  }

  /**
   * Get the facet filter related to this instance
   * @return facet filter
   */

  def facetFilter: String
}

object AbstractFacetPartition {

  /**
   * Generate a set of partitions from values retrieved from a facetable field
   * @param maybeFilter optional overall filter
   * @param maybeSelect optional list of selection fields
   * @param facetField facet field
   * @param facets facet field values
   * @return a collection of Search partitions
   */

  def createCollection(maybeFilter: Option[String],
                       maybeSelect: Option[Seq[String]],
                       facetField: SearchField,
                       facets: Seq[Any]): Seq[AbstractFacetPartition] = {

    val valueFormatter = FilterValueFormatters.forType(facetField.getType)
    val facetFieldName: String = facetField.getName
    val facetStringValues: Seq[String] = facets.map {
      valueFormatter.format
    }

    // Create as many partitions as the number of retrieved facet values
    val partitionsForFacetsValues: Seq[AbstractFacetPartition] = facetStringValues
      .zipWithIndex.map {
        case (value, partitionId) =>
          FacetValuePartition(
            partitionId,
            maybeFilter,
            maybeSelect,
            facetFieldName,
            value
          )
      }

    // Add another partition for either null values or other facet values
    val partitionForEitherNullOrOtherFacetValues = FacetNullValuePartition(
      maybeFilter,
      maybeSelect,
      facetFieldName,
      facetStringValues
    )

    partitionsForFacetsValues :+ partitionForEitherNullOrOtherFacetValues
  }
}