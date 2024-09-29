package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchField
import com.azure.search.documents.models.{FacetResult, SearchOptions}
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.config.{ConfigException, SearchConfig}
import com.github.jarol.azure.search.spark.sql.connector.core.schema.{SearchFieldFeature, toSearchFieldOperations}
import com.github.jarol.azure.search.spark.sql.connector.read.ReadConfig
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsOperations._

import java.util

/**
 * Faceted partitioner.
 *
 * Given a field <b>f1</b> which is filterable and facetable, it will generate partitions according to the following behavior
 *  - if a value of <b>n</b> is given for [[ReadConfig.NUM_PARTITIONS_CONFIG]], it will generate <b>n</b> partitions
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
   *  - the value related to key [[ReadConfig.NUM_PARTITIONS_CONFIG]]
   *  - the number of default facets retrieved by the Azure Search API
   *
   * Each partition should contain o non-overlapping filter
   *
   * @throws ConfigException if facet field is not facetable and filterable
   * @return a collection of Search partitions
   */

  @throws[ConfigException]
  override def createPartitions(): util.List[SearchPartition] = {

    val partitionerOptions: SearchConfig = readConfig.partitionerOptions
    val facetFieldName: String = partitionerOptions.unsafelyGet(ReadConfig.FACET_FIELD_CONFIG)
    val facetPartitions: Option[Int] = partitionerOptions.getAs(ReadConfig.NUM_PARTITIONS_CONFIG, Integer.parseInt)

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
   * Get the partition list
   * @param field facetable Search field
   * @param facets field facets
   * @return a list of [[SearchPartition]]
   */

  private def getPartitionList(
                                field: SearchField,
                                facets: Seq[FacetResult]
                              ): util.List[SearchPartition] = {

    val partitions = AbstractFacetPartition.createCollection(
      readConfig.filter,
      readConfig.select,
      field,
      facets.map(_.getAdditionalProperties.get("value"))
    )

    JavaScalaConverters.seqToList(partitions)
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
    // [a] if query param is defined, facet = join facetField and query param using comma
    // [b] if query params is empty facet = facetField
    val facet: String = partitions.map {
      value => s"$facetField,count:${value - 1}"
    }.getOrElse(facetField)

    val facets = readConfig.search(
        new SearchOptions()
          .setFilter(readConfig.filter)
          .setSelect(readConfig.select)
          .setFacets(facet)
      ).getFacets.get(facetField)

    JavaScalaConverters.listToSeq(facets)
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
    val maybeExistingField = fields.collectFirst {
      case sf if sf.getName.equalsIgnoreCase(name) => sf
    }.toRight().left.map {
      _ => IllegalSearchFieldException.nonExisting(name)
    }.right.flatMap(evaluateExistingCandidate)

    // Map left side to a ConfigException
    maybeExistingField.left.map {
      cause => new ConfigException(
        ReadConfig.FACET_FIELD_CONFIG,
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

  private def evaluateExistingCandidate(candidate: SearchField): Either[IllegalSearchFieldException, SearchField] = {

    val facetable = candidate.isEnabledFor(SearchFieldFeature.FACETABLE)
    val filterable = candidate.isEnabledFor(SearchFieldFeature.FILTERABLE)
    if (facetable && filterable) {
      Right(candidate)
    } else {
      val nonEnabledFeature = if (!facetable) SearchFieldFeature.FACETABLE else SearchFieldFeature.FILTERABLE
      Left(
        IllegalSearchFieldException.notEnabledFor(
          candidate.getName,
          nonEnabledFeature
        )
      )
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
            new ConfigException(
              ReadConfig.NUM_PARTITIONS_CONFIG,
              value,
              "should be greater than 1"
            )
          )
        }
      case None => Right(partitions)
    }
  }
}