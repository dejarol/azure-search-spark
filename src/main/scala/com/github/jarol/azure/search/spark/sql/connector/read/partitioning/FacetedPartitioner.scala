package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchField
import com.azure.search.documents.models.{FacetResult, SearchOptions}
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.clients.JavaClients
import com.github.jarol.azure.search.spark.sql.connector.config.{ConfigException, IOConfig, ReadConfig, SearchConfig}
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsOperations._

import java.util
import java.util.stream.Collectors

case class FacetedPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  override def generatePartitions(): util.List[SearchPartition] = {

    val partitionerOptions: SearchConfig = readConfig.partitionerOptions
    val facet: String = partitionerOptions.unsafelyGet(ReadConfig.PARTITIONER_OPTIONS_FACET_CONFIG)
    val facetQueryParam: Option[String] = partitionerOptions.get(ReadConfig.PARTITIONER_OPTIONS_FACET_QUERY_PARAMETER_CONFIG)
    val searchFields: Seq[SearchField] = JavaScalaConverters.listToSeq(
      JavaClients.forIndex(readConfig)
        .getIndex(readConfig.getIndex)
        .getFields
    )

    val eitherExceptionOrFacets: Either[ConfigException, util.List[FacetResult]] = for {
      facetField <- FacetedPartitioner.isEligibleFacet(searchFields, facet)
      facetResults <- FacetedPartitioner.allFacetsAreValid(readConfig, facetField.getName, facetQueryParam)
    } yield facetResults

    eitherExceptionOrFacets match {
      case Left(value) => throw value
      case Right(value) => generateFacetPartitions(facet, value)
    }
  }

  private def generateFacetPartitions(facet: String, results: util.List[FacetResult]): util.List[SearchPartition] = {

    results.stream()
      .map[SearchPartitionScalaImpl] {
        fr => {
          val value: Any = fr.getAdditionalProperties.get("value")
          val filterValueAsString: String = value match {
            case s: String => s"'$s'"
            case _ => String.valueOf(value)
          }

          val facetFilter: String = s"$facet eq $filterValueAsString"
          val filter: Option[String] = readConfig.filter.map {
            f => f"$f and $facetFilter"
          }.orElse(Some(facetFilter))

          SearchPartitionScalaImpl(
            filter,
            readConfig.select
          )
      }
    }.collect(Collectors.toList[SearchPartition])
  }
}

object FacetedPartitioner {

  final val FIELD_DOES_NOT_EXIST_ERROR_MSG = "Field does not exist"
  final val FIELD_NOT_FACETABLE_ERROR_MSG = "Field not facetable or filterable"

  protected[partitioning] def isEligibleFacet(searchFields: Seq[SearchField], facet: String): Either[ConfigException, SearchField] = {

    // Detect a field with same name
    val maybeSearchField: Option[SearchField] = searchFields
      .collectFirst {
        case sf if sf.getName.equalsIgnoreCase(facet) =>
          sf
    }

    // Evaluate if it's both filterable and facetable
    val maybeFacetableAndFilterableField: Option[SearchField] = maybeSearchField
      .filter {
        sf => sf.isFacetable && sf.isFilterable
    }

    // If so, return a Right
    if (maybeFacetableAndFilterableField.isDefined) {
      Right(maybeFacetableAndFilterableField.get)
    } else {

      // Set the error message
      val message = if (maybeSearchField.isEmpty) {
        FacetedPartitioner.FIELD_DOES_NOT_EXIST_ERROR_MSG
      } else FacetedPartitioner.FIELD_NOT_FACETABLE_ERROR_MSG

      // create a ConfigException
      Left(
        new ConfigException(
          ReadConfig.PARTITIONER_OPTIONS_FACET_CONFIG,
          facet,
          message
        )
      )
    }
  }

  protected[partitioning] def allFacetsAreValid(readConfig: ReadConfig,
                                                facet: String,
                                                facetQueryParam: Option[String]): Either[ConfigException, util.List[FacetResult]] = {

    val fullFacet: String = facetQueryParam.map {
      qp => s"$facet,$qp"
    }.getOrElse(facet)

    val facetResults: util.List[FacetResult] = JavaClients.doSearch(
        readConfig,
        new SearchOptions()
          .setFilter(readConfig.filter)
          .setFacets(fullFacet)
      ).getFacets.get(facet)

    val allFacetsCountAreBelowSkipLimit = facetResults.stream().allMatch {
      _.getCount <= IOConfig.SKIP_LIMIT
    }

    if (allFacetsCountAreBelowSkipLimit) {
      Right(facetResults)
    } else {
      Left(
        new ConfigException(
          ReadConfig.PARTITIONER_OPTIONS_FACET_CONFIG,
          facet,
          s"Facet $facet is not valid (some facets have more than ${IOConfig.SKIP_LIMIT} documents"
        )
      )
    }
  }
}