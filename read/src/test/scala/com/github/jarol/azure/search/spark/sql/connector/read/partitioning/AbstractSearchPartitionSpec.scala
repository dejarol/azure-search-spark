package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.read.SearchOptionsBuilder

class AbstractSearchPartitionSpec
  extends BasicSpec {

  import AbstractSearchPartitionSpec._

  /**
   * Create a partition instance
   * @param inputFilter input (user-specified) filter
   * @param partFilter partition filter
   * @return a partition instance
   */

  private def createPartitionAndGetFilter(
                                           inputFilter: String,
                                           partFilter: Option[String]
                                         ): String = {

    new AbstractSearchPartition(
      0,
      SimpleOptionsBuilder(
        Seq(inputFilter)
      )
    ) {
      override protected[partitioning] def partitionFilter: Option[String] = partFilter
    }.getSearchOptions.getFilter
  }

  describe(anInstanceOf[AbstractSearchPartition]) {
    describe(SHOULD) {
      describe("create a filter that") {
        it("combines partition filter with the builder filter") {

          val (first, second) = ("a eq 1", "b eq 2")
          createPartitionAndGetFilter(first, None) shouldBe first
          createPartitionAndGetFilter(first, Some(second)) shouldBe s"$first,$second"
        }
      }
    }
  }
}

object AbstractSearchPartitionSpec {

  /**
   * An implementation of options builder
   * <br>
   * It creates a null filter if there are no cumulated filters, otherwise it will combine
   * these into a single filter
   * @param cumulatedFilters cumulated filters
   */

  // TODO: fix
  case class SimpleOptionsBuilder(private val cumulatedFilters: Seq[String])
    extends SearchOptionsBuilder {
    private lazy val filter: String = if (cumulatedFilters.isEmpty) null else cumulatedFilters.mkString(",")
    override def buildOptions(): SearchOptions = new SearchOptions().setFilter(filter)
    override def withFilter(other: String): SearchOptionsBuilder = this.copy(cumulatedFilters = cumulatedFilters :+ other)
  }
}