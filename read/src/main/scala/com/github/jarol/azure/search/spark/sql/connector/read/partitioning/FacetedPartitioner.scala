package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchField
import com.azure.search.documents.models.FacetResult
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.config.ConfigException
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SearchFieldFeature, toSearchFieldOperations, toSearchTypeOperations}
import com.github.jarol.azure.search.spark.sql.connector.read.config.ReadConfig

import java.util.{List => JList}

/**
 * Faceted partitioner.
 * <br>
 * Given a field <b>f1</b> that is filterable and facetable, it will generate partitions according to the following behavior
 *  - if a value of <b>n</b> is given for [[SearchPartitioner.NUM_PARTITIONS_CONFIG]], it will generate <b>n</b> partitions
 *    where partition <b>i = 0, ..., n - 1</b> will contain documents where <b>f1</b> is equal to the <b>i-th</b>
 *    most frequent value of field  <b>f1</b>,
 *    and a partition for all documents where <b>f1</b> is null or does not meet one of the  <b>n - 1</b> most frequent values
 *  - otherwise, the number of partitions will be the default number of facets returned by the Azure Search API
 *
 * Suitable for cases where there exists a filterable and facetable field with few distinct values
 *
 * @param readConfig read configuration
 */

case class FacetedPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  /**
   * Generate a number of partitions equal to
   *  - the value related to key [[SearchPartitioner.NUM_PARTITIONS_CONFIG]]
   *  - the number of default facets retrieved by the Azure Search API
   *
   * Each partition should contain o non-overlapping filter
   *
   * @throws ConfigException if facet field is not facetable and filterable
   * @return a collection of Search partitions
   */

  @throws[ConfigException]
  override def createPartitions(): JList[SearchPartition] = {

    val facetFieldName: String = partitionerOptions.unsafelyGet(SearchPartitioner.FACET_FIELD_CONFIG, Some(ReadConfig.PARTITIONER_OPTIONS_PREFIX), None)
    val facetPartitions: Option[Int] = partitionerOptions.getAs(SearchPartitioner.NUM_PARTITIONS_CONFIG, Integer.parseInt)

    // Either a ConfigException or facet results
    val either: Either[ConfigException, (SearchField, Seq[FacetResult])] = for {
      facetableField <- FacetedPartitioner.getCandidateFacetField(facetFieldName, readConfig.getSearchIndexFields)
      partitions <- FacetedPartitioner.evaluatePartitionNumber(facetPartitions)
    } yield (
      facetableField,
      getFacetResults(
        facetableField.getName,
        partitions
      )
    )

    either match {
      case Left(value) => throw value
      case Right((field, facets)) => getPartitionList(field, facets)
    }
  }

  /**
   * Retrieve a number of [[FacetResult]](s) for a search field. A facet result contains value cardinality
   * (i.e. number of documents with such field value) for the n most frequent values of a search field
   * @param facetField name of facetable field
   * @param partitions number of values to retrieve. If not provided, the default search value will be used
   * @return a collection of [[FacetResult]]
   */

  private def getFacetResults(
                               facetField: String,
                               partitions: Option[Int]
                             ): Seq[FacetResult] = {

    // Compose the facet
    // [a] if query param is defined, facet is the combination of facetField and query param using comma
    // [b] if query params is empty facet = facetField
    val facetExpression: String = partitions.map {
      value => s"$facetField,count:${value - 1}"
    }.getOrElse(facetField)

    readConfig.getFacets(facetField, facetExpression)
  }

  /**
   * Get the partition list
   * @param field facetable Search field
   * @param facets field facets
   * @return a list of [[SearchPartition]]
   */

  private def getPartitionList(
                                field: SearchField,
                                facets: Seq[FacetResult]
                              ): JList[SearchPartition] = {

    val partitions = AbstractFacetPartition.createCollection(
      field,
      facets.map(_.getAdditionalProperties.get("value"))
    )

    JavaScalaConverters.seqToList(partitions)
  }
}

object FacetedPartitioner {

  /**
   * Get candidate field for faceting
   * <br>
   * A field is eligible for faceting if
   *  - it exists
   *  - it's both facetable and filterable
   *
   * If any of the previous conditions do not hold, a [[ConfigException]] will be returned
   * @param name name
   * @param fields collection of Search fields
   * @return either a [[ConfigException]] or the candidate field
   */

  private[partitioning] def getCandidateFacetField(
                                                    name: String,
                                                    fields: Seq[SearchField]
                                                  ): Either[ConfigException, SearchField] = {

    // Collect the namesake field and evaluate it (if any)
    val maybeExistingField: Either[IllegalSearchFieldException, SearchField] = fields.collectFirst {
      case sf if sf.getName.equalsIgnoreCase(name) => sf
    }.toRight(()).left.map {
      _ => IllegalSearchFieldException.nonExisting(name)
    }.right.flatMap(evaluateExistingCandidate)

    // Map left side to a ConfigException
    maybeExistingField.left.map {
      cause => ConfigException.forIllegalOptionValue(
        SearchPartitioner.FACET_FIELD_CONFIG,
        name,
        cause
      )
    }
  }

  /**
   * Evaluate if an existing Search field is a good candidate for faceting
   * <br>
   A field is eligible for faceting if it's both facetable and filterable
   * @param candidate candidate field
   * @return either a [[ConfigException]] or the candidate
   */

  private def evaluateExistingCandidate(candidate: SearchField): Either[IllegalFacetableFieldException, SearchField] = {

    val facetable = candidate.isEnabledFor(SearchFieldFeature.FACETABLE)
    val filterable = candidate.isEnabledFor(SearchFieldFeature.FILTERABLE)
    val facetableType = candidate.getType.isCandidateForFaceting
    if (facetable && filterable && facetableType) {
      Right(candidate)
    } else {

      val exception = if (!facetableType) {
        IllegalFacetableFieldException.forInvalidType(candidate)
      } else {
        val nonEnabledFeature = if (!facetable) SearchFieldFeature.FACETABLE else SearchFieldFeature.FILTERABLE
        IllegalFacetableFieldException.forMissingFeature(candidate, nonEnabledFeature)
      }

      Left(exception)
    }
  }

  /**
   * Evaluate if the provided partition number is valid
   * @param partitions partition number (optional)
   * @return either a [[ConfigException]] or the input itself
   */

  private[partitioning] def evaluatePartitionNumber(partitions: Option[Int]): Either[ConfigException, Option[Int]] = {

    partitions match {
      case Some(value) =>
        if (value > 1) {
          Right(partitions)
        } else {
          Left(
            ConfigException.forIllegalOptionValue(
              SearchPartitioner.NUM_PARTITIONS_CONFIG,
              s"$value",
              "should be greater than 1"
            )
          )
        }
      case None => Right(partitions)
    }
  }
}