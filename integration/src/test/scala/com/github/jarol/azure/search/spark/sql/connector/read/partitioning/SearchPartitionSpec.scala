package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.SearchTestUtils
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.models._

class SearchPartitionSpec
  extends AbstractSearchPartitionITSpec {

  private lazy val indexName = "search-partition-spec"
  private lazy val partition = new SearchPartition {
    override def getPartitionId: Int = 0
    override def getSearchOptions: SearchOptions = new SearchOptions()
      .setFilter("value ne null")
  }

  private lazy val valueNeNull: PairBean[Int] => Boolean = _.value.isDefined
  private lazy val documents: Seq[PairBean[Int]] = Seq(
    PairBean("hello", None),
    PairBean(1),
    PairBean(2),
    PairBean("world", None)
  )

  override def beforeAll(): Unit = {

    super.beforeAll()
    createIndexFromSchemaOf[PairBean[Int]](indexName)
    writeDocuments[PairBean[Int]](indexName, documents)(PairBean.serializerFor[Int])
  }

  describe(anInstanceOf[SearchPartition]) {
    describe(SHOULD) {
      it("retrieve all documents matching search options") {

        val actualDocuments = SearchTestUtils.getPartitionDocuments(
          partition,
          getSearchClient(indexName),
          null
        )

        val expectedDocs = documents.filter(valueNeNull)
        actualDocuments should have size expectedDocs.size
        val actualIds = JavaScalaConverters.listToSeq(actualDocuments).map {
          _.get("id").asInstanceOf[String]
        }

        actualIds should contain theSameElementsAs expectedDocs.map(_.id)
      }

      it("get the number of retrieved documents") {

        partition.getCountPerPartition(
          getSearchClient(indexName),
          null
        ) shouldBe documents.count(valueNeNull)
      }
    }
  }
}
