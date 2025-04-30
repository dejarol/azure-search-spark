package io.github.dejarol.azure.search.spark.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchField
import com.azure.search.documents.models.FacetResult
import io.github.dejarol.azure.search.spark.connector.core.NoSuchSearchFieldException
import io.github.dejarol.azure.search.spark.connector.core.config.ConfigException
import io.github.dejarol.azure.search.spark.connector.core.schema._
import io.github.dejarol.azure.search.spark.connector.core.utils.StringUtils
import io.github.dejarol.azure.search.spark.connector.read.config.ReadConfig
import io.github.dejarol.azure.search.spark.connector.read.partitioning.AbstractFacetPartition.FacetToStringFunction

object FacetedPartitionerFactory
  extends PartitionerFactory {

  override def createPartitioner(readConfig: ReadConfig): SearchPartitioner = {

    // Retrieve facet field name and number of partitions
    val partitionerOptions = readConfig.partitionerOptions
    val facetFieldName: String = partitionerOptions.unsafelyGet(
      SearchPartitioner.FACET_FIELD_CONFIG,
      Some(ReadConfig.PARTITIONER_OPTIONS_PREFIX),
      None
    )

    val facetPartitions: Option[Int] = partitionerOptions.getAs(
      SearchPartitioner.NUM_PARTITIONS_CONFIG,
      Integer.parseInt
    )

    val x = for {
      searchField <- getCandidateFacetField(facetFieldName, readConfig.getSearchIndexFields)
      partitions <- evaluatePartitionNumber(facetPartitions)
    } yield (
      searchField,
      getFacetResults(readConfig, searchField.getName, partitions)
    )

    x match {
      case Left(value) => ???
      case Right((field, facets)) => FacetedPartitionerV2(
        field.getName,
      )
    }
  }

  /**
   * Get the function corresponding to a Search type
   * @param searchField facetable field
   * @throws IllegalStateException for Search types (should not occur)
   * @return a function for value formatting
   */

  @throws[IllegalStateException]
  private def getFunction(searchField: SearchField): FacetToStringFunction = {

    val searchFieldDataType = searchField.getType
    if (searchFieldDataType.isString) {
      new FacetToStringFunction {
        override def apply(v1: Any): String = StringUtils.singleQuoted(v1.asInstanceOf[String])
      }
    } else if (searchFieldDataType.isNumeric) {
      new FacetToStringFunction {
        override def apply(v1: Any): String = String.valueOf(v1)
      }
    } else {
      throw new IllegalStateException(f"No facet to string function defined for $searchField")
    }
  }

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
    val maybeExistingField: Either[Throwable, SearchField] = fields.collectFirst {
      case sf if sf.getName.equalsIgnoreCase(name) => sf
    } match {
      case Some(value) => evaluateExistingCandidate(value)
      case None => Left(
        new NoSuchSearchFieldException(name)
      )
    }

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

  /**
   * Retrieve a number of [[FacetResult]](s) for a search field. A facet result contains value cardinality
   * (i.e. number of documents with such field value) for the n most frequent values of a search field
   * @param facetField name of facetable field
   * @param partitions number of values to retrieve. If not provided, the default search value will be used
   * @return a collection of [[FacetResult]]
   */

  private def getFacetResults(
                               readConfig: ReadConfig,
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
}
