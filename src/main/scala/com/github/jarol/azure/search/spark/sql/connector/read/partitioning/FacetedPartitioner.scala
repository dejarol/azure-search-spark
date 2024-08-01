package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchField
import com.azure.search.documents.models.{FacetResult, SearchOptions}
import com.github.jarol.azure.search.spark.sql.connector.clients.ClientFactory
import com.github.jarol.azure.search.spark.sql.connector.config.{ConfigException, ReadConfig, SearchConfig}
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsOperations._
import com.github.jarol.azure.search.spark.sql.connector.{AzureSparkException, JavaScalaConverters}

import java.util
import scala.util.Try

/**
 * Faceted partitioner.
 *
 * Given a field <b>f1</b> which is filterable and facetable, it will generate partitions according to the following behavior
 *  - if a value of <b>n</b> is given for [[ReadConfig.PARTITIONER_OPTIONS_FACET_PARTITIONS]], it will generate <b>n</b> partitions
 *  where partition <b>i = 0, ..., n - 1</b> will contain documents where <b>f1</b> is equal to the <b>i-th</b>
 *  most frequent value of field  <b>f1</b>,
 *  and a partition for all documents where <b>f1</b> is null or does not meet one of the  <b>n - 1</b> most frequent values
 *  - otherwise, the number of partitions will be the default number of facets returned by the Azure Search API
 *
 * Suitable for cases where there exists a filterable and facetable field with few distinct values
 * @param readConfig read configuration
 */

case class FacetedPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  /**
   * Generate a number of partitions equal to
   *  - the value related to key [[ReadConfig.PARTITIONER_OPTIONS_FACET_PARTITIONS]]
   *  - the number of default facets retrieved by the Azure Search API
   *
   * Each partition should contain o non-overlapping filter
   * @throws ConfigException if facet field is not facetable and retrievable
   * @return a collection of Search partitions
   */

  @throws[ConfigException]
  override def createPartitions(): util.List[SearchPartition] = {

    val partitionerOptions: SearchConfig = readConfig.partitionerOptions
    val facetFieldName: String = partitionerOptions.unsafelyGet(ReadConfig.PARTITIONER_OPTIONS_FACET_CONFIG)
    val facetPartitions: Option[Int] = partitionerOptions.getAs(ReadConfig.PARTITIONER_OPTIONS_FACET_PARTITIONS, Integer.parseInt)

    // Retrieve facet result and generate partitions
    FacetedPartitioner.getFacetResults(readConfig, facetFieldName, facetPartitions) match {
      // case Left: throw handled exception
      case Left(value) => throw value
      // case Right: generate the partitions
      case Right(value) => FacetedPartitioner.generatePartitions(
        getFacetFieldType(facetFieldName),
        JavaScalaConverters.listToSeq(value).map {
          _.getAdditionalProperties.get("value")
        },
        readConfig
      )
    }
  }

  /** Retrieve the Search field with given name
   * @param name field name
   * @throws AzureSparkException if field cannot be retrieved
   * @return Search field with given name
   */

  @throws[AzureSparkException]
  private def getFacetFieldType(name: String): SearchField = {

    // Retrieve the search field with given name
    JavaScalaConverters.listToSeq(
      ClientFactory.searchIndex(readConfig).getFields
    ).collectFirst {
      case sf if sf.getName.equalsIgnoreCase(name) => sf
    } match {
      case Some(value) => value
      case None => throw new AzureSparkException(s"Could not retrieve information for facet field $name")
    }
  }
}

object FacetedPartitioner {

  /**
   * Retrieve a number of [[FacetResult]](s) for a search field. A facet result contains value cardinality
   * (i.e. number of documents with such field value) for the n most frequent values of a search field
   * @param config read configuration
   * @param facetField name of facetable field
   * @param count number of values to retrieve. If not provided, the default search value will be used
   * @return either a [[ConfigException]] if selected facet field does not exist or it's not facetable, or a list of [[FacetResult]]
   */

  private def getFacetResults(config: ReadConfig,
                              facetField: String,
                              count: Option[Int]): Either[ConfigException, util.List[FacetResult]] = {

    // Compose the facet
    // [a] if query param is defined, facet = join facetField and query param using comma
    // [b] if query params is empty facet = facetField
    val facet: String = count.map {
      partitions => s"$facetField,count:${partitions - 1}"
    }.getOrElse(facetField)

    // Try to retrieve facet results, mapping the exception to a ConfigException
    Try {
      ClientFactory.doSearch(
        config,
        new SearchOptions()
          .setFilter(config.filter)
          .setSelect(config.select)
          .setFacets(facet)
      ).getFacets.get(facetField)
    }.toEither.left.map {
      throwable =>
        new ConfigException(
          ReadConfig.PARTITIONER_OPTIONS_FACET_CONFIG,
          facet,
          throwable
        )
    }
  }

  /**
   * Combine two filters into one using the <b>and</b> logical operator.
   * The output filter will be the combination of the two (if the second is defined), otherwise only the first filter
   * @param first first filter
   * @param second second filter (optional)
   * @return the combination of the teo filters
   */

  protected[partitioning] def combineFilters(first: String, second: Option[String]): String = {

    second match {
      case Some(value) => s"$first and $value"
      case None => first
    }
  }

  /**
   * Generate a set of partitions exploiting values of a facetable field
   * @param facetField facet field
   * @param facets facet field values
   * @param readConfig read config
   * @return a collection of Search partitions
   */

  protected[partitioning] def generatePartitions(facetField: SearchField,
                                                 facets: Seq[Any],
                                                 readConfig: ReadConfig): util.List[SearchPartition] = {

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

    val allSearchPartitions: Seq[SearchPartition] = (facetPartitionFilters :+ nullOrNotInOtherFacets)
      .zipWithIndex.map { case (facetFilter, partitionId) =>
        ScalaSearchPartition(
          partitionId,
          Some(combineFilters(facetFilter, readConfig.filter)),
          readConfig.select
        )
    }

    JavaScalaConverters.seqToList(allSearchPartitions)
  }
}