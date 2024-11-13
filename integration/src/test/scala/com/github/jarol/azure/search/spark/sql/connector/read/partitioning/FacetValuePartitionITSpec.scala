package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.models._

class FacetValuePartitionITSpec
  extends SearchPartitionITSpec {

  private lazy val indexName = "facet-partition-index"
  private lazy val facetFieldName = "value"
  private lazy val (john, jane, matchingId) = ("john", "jane", "1")
  private lazy val nameEqJohn: PairBean[String] => Boolean = _.value.exists {
    _.equals(john)
  }

  /**
   * Assert that a proper number of document per partition is retrieved
   * @param documents input documents
   * @param filter partition filter
   * @param facet partition facet
   * @param expectedCountPredicate predicate for computing the expected number of documents
   */

  private def assertCountPerPartition(
                                       documents: Seq[PairBean[String]],
                                       filter: Option[String],
                                       facet: String,
                                       expectedCountPredicate: PairBean[String] => Boolean
                                     ): Unit = {

    // Create index and write data
    createIndexFromSchemaOf[PairBean[String]](indexName)
    writeDocuments(indexName, documents)(PairBean.serializerFor[String])

    // Get actual count
    val actualCount = FacetValuePartition(0, filter, None, facetFieldName, f"'$facet'")
      .getCountPerPartition(
        getSearchClient(indexName)
      )

    actualCount shouldBe documents.count(expectedCountPredicate)
  }

  describe(anInstanceOf[FacetValuePartition]) {
    describe(SHOULD) {
      describe("retrieve values matching") {
        it("only facet value") {

          val documents: Seq[PairBean[String]] = Seq(
            PairBean(john),
            PairBean(john),
            PairBean(jane)
          )

          assertCountPerPartition(documents, None, john, nameEqJohn)
        }

        it("both filter and facet value") {

          dropIndexIfExists(indexName, sleep = true)
          val documents: Seq[PairBean[String]] = Seq(
            PairBean(matchingId, Some(john)),
            PairBean("2", Some(john))
          )

          val nameEqJohnAndMatchingId: PairBean[String] => Boolean = p => nameEqJohn(p) && p.id.equals(matchingId)
          assertCountPerPartition(documents, Some(s"id eq '$matchingId'"), john, nameEqJohnAndMatchingId)
        }
      }
    }
  }
}
