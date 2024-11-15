package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.{SearchField, SearchFieldDataType}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.toSearchTypeOperations
import com.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils

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

abstract class AbstractFacetPartition(
                                       override protected val partitionId: Int,
                                       override protected val inputFilter: Option[String],
                                       override protected val maybeSelect: Option[Seq[String]],
                                       protected val facetFieldName: String
                                     )
  extends AbstractSearchPartition(partitionId, inputFilter, maybeSelect) {

  override final def getSearchFilter: String = {

    inputFilter match {
      case Some(value) => s"$value and ($facetFilter)"
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
   * Trait for formatting facet values to string
   * <br>
   * Formatted values will be used for filtering documents within partitions
   */

  private sealed trait FacetToStringFunction extends (Any => String)

  /**
   * Generate a set of partitions from values retrieved from a facetable field
   * @param maybeFilter optional overall filter
   * @param maybeSelect optional list of selection fields
   * @param facetField facet field
   * @param facets facet field values
   * @return a collection of Search partitions
   */

  def createCollection(
                        maybeFilter: Option[String],
                        maybeSelect: Option[Seq[String]],
                        facetField: SearchField,
                        facets: Seq[Any]
                      ): Seq[AbstractFacetPartition] = {

    val toStringFunction = getFunction(facetField.getType)
    val facetFieldName: String = facetField.getName
    val facetStringValues: Seq[String] = facets.map(toStringFunction)

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

  /**
   * Get the function corresponding to a Search type
   * @param searchType facetable field type
   * @throws IllegalStateException for Search types (should not occur)
   * @return a function for value formatting
   */

  @throws[IllegalStateException]
  private def getFunction(searchType: SearchFieldDataType): FacetToStringFunction = {

    if (searchType.isString) {
      new FacetToStringFunction {
        override def apply(v1: Any): String = StringUtils.singleQuoted(v1.asInstanceOf[String])
      }
    } else if (searchType.isNumeric) {
      new FacetToStringFunction {
        override def apply(v1: Any): String = String.valueOf(v1)
      }
    } else {
      throw new IllegalStateException(f"No facet to string function defined for $searchType")
    }
  }
}