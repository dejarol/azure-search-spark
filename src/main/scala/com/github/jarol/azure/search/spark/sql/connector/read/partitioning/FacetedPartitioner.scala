package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.indexes.models.SearchField
import com.github.jarol.azure.search.spark.sql.connector.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.clients.ClientFactory
import com.github.jarol.azure.search.spark.sql.connector.config.{ConfigException, ReadConfig, SearchConfig}

import java.util

case class FacetedPartitioner(override protected val readConfig: ReadConfig)
  extends AbstractSearchPartitioner(readConfig) {

  override def generatePartitions(): util.List[SearchPartition] = {

    val partitionerOptions: SearchConfig = readConfig.partitionerOptions
    val facetName: String = partitionerOptions.unsafelyGet(ReadConfig.PARTITIONER_OPTIONS_FACET_CONFIG)
    val maybeFieldToFacet: Option[SearchField] = JavaScalaConverters.listToSeq(
      ClientFactory.indexClient(readConfig)
      .getIndex(readConfig.getIndex)
      .getFields
    ).collectFirst {
      case sf if sf.getName.equalsIgnoreCase(facetName) &&
        sf.isFacetable &&
        sf.isFilterable => sf
    }

    maybeFieldToFacet match {
      case Some(value) => ???
      case None => throw new ConfigException(
        ReadConfig.PARTITIONER_OPTIONS_FACET_CONFIG,
        facetName,
        new IllegalArgumentException("Field not facetable or filterable")
      )
    }
  }

  private def generateFacetPartitions(searchField: SearchField): util.List[SearchPartition] = {

    val facetName: String = searchField.getName
    /*
    ClientFactory.searchClient(readConfig)
      .search(
        null,
        new SearchOptions()
          .setFilter(readConfig.filter)
          .setFacets(facetName),
      Context.NONE
      ).getFacets.get(facetName).stream().map(
      facet => facet.getAdditionalProperties.get("value")
      )

     */

    util.Collections.emptyList()
  }
}
