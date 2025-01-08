package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils
import com.github.jarol.azure.search.spark.sql.connector.models._
import com.github.jarol.azure.search.spark.sql.connector.{DocumentIDGetter, DocumentSerializer}

class FacetValuePartitionITSpec
  extends SearchPartitionITSPec {

  private lazy val (indexName, facetField) = ("facet-value-partition-index", "value")
  private lazy val (john, jane, matchingId) = ("john", "jane", "1")
  private lazy val nameEqJohn: PairBean[String] => Boolean = _.value.exists {
    _.equals(john)
  }

  private implicit lazy val serializer: DocumentSerializer[PairBean[String]] = PairBean.serializerFor[String]
  private implicit lazy val idGetter: DocumentIDGetter[PairBean[String]] = idGetterFor()

  override final def beforeAll(): Unit = {

    // Clean up Search service from existing indexes and create test index
    super.beforeAll()
    createIndexFromSchemaOf[PairBean[String]](indexName)
  }

  /**
   * Create a partition instance
   * @param inputFilter filter
   * @param facetField facet field
   * @param facet facet value
   * @return a partition
   */

  private def createPartition(
                               inputFilter: Option[String],
                               facetField: String,
                               facet: String
                             ): SearchPartition = {

    // TODO: fix method for adding predicates
    FacetValuePartition(
      0,
      inputFilter,
      None,
      Seq.empty,
      facetField,
      StringUtils.singleQuoted(facet)
    )
  }

  describe(anInstanceOf[FacetValuePartition]) {
    describe(SHOULD) {
      describe("retrieve documents matching") {
        it("only facet value") {

          val documents: Seq[PairBean[String]] = Seq(
            PairBean(john),
            PairBean(john),
            PairBean(jane)
          )

          // Assertion
          assertCountPerPartition[PairBean[String]](
            documents,
            indexName,
            createPartition(None, facetField, john),
            nameEqJohn
          )
        }

        it("both filter and facet value") {

          truncateIndex(indexName)
          val documents: Seq[PairBean[String]] = Seq(
            PairBean(matchingId, Some(john)),
            PairBean("2", Some(john))
          )

          // Assertion
          val nameEqJohnAndMatchingId: PairBean[String] => Boolean = p => nameEqJohn(p) && p.id.equals(matchingId)
          assertCountPerPartition(
            documents,
            indexName,
            createPartition(Some(s"id eq '$matchingId'"), facetField, john),
            nameEqJohnAndMatchingId
          )
        }
      }
    }
  }
}
