package io.github.dejarol.azure.search.spark.connector.read.partitioning

import io.github.dejarol.azure.search.spark.connector.core.utils.StringUtils
import java.time.LocalDate

class FacetValuePartitionITSpec
  extends AbstractSearchPartitionITSpec {

  private lazy val (john, jane) = ("john", "jane")
  private lazy val (indexName, facetField) = ("facet-value-partition-index", "stringValue")
  private lazy val stringValueEqJohn: PushdownBean => Boolean = _.stringValue.exists(_.equals(john))
  private lazy val documents: Seq[PushdownBean] = Seq(
    PushdownBean(Some(john), Some(1), None),
    PushdownBean(Some(john), Some(1), Some(LocalDate.now())),
    PushdownBean(None, None, None),
    PushdownBean(Some(jane), Some(1), Some(LocalDate.now()))
  )

  override final def beforeAll(): Unit = {

    // Clean up Search service from existing indexes and create test index
    super.beforeAll()
    createIndexFromSchemaOf[PushdownBean](indexName)
    writeDocuments(indexName, documents)
  }

  /**
   * Create a partition instance
   * @param facetField  facet field
   * @param facet       facet value
   * @return a partition
   */

  private def createPartition(
                               facetField: String,
                               facet: String
                             ): FacetValuePartition = {

    FacetValuePartition(
      0,
      facetField,
      facet
    )
  }

  describe(anInstanceOf[FacetValuePartition]) {
    describe(SHOULD) {
      it("create a facet filter related to given value") {

        val (fieldName, fieldValue) = ("type", StringUtils.singleQuoted("LOAN"))
        createPartition(fieldName, fieldValue).getPartitionFilter shouldBe s"$fieldName eq $fieldValue"
      }

      describe("retrieve documents matching") {
        it("only facet value") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(facetField, StringUtils.singleQuoted(john)),
            stringValueEqJohn
          )
        }
      }
    }
  }
}
