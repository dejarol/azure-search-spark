package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils
import com.github.jarol.azure.search.spark.sql.connector.models._
import com.github.jarol.azure.search.spark.sql.connector.read.filter.ODataExpression

import java.time.LocalDate

class FacetValuePartitionITSpec
  extends SearchPartitionITSPec {

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
   *
   * @param inputFilter filter
   * @param facetField  facet field
   * @param facet       facet value
   * @return a partition
   */

  private def createPartition(
                               inputFilter: Option[String],
                               facetField: String,
                               facet: String,
                               pushedPredicates: Seq[ODataExpression]
                             ): FacetValuePartition = {

    FacetValuePartition(
      0,
      inputFilter,
      None,
      pushedPredicates,
      facetField,
      StringUtils.singleQuoted(facet)
    )
  }

  describe(anInstanceOf[FacetValuePartition]) {
    describe(SHOULD) {
      it("create a facet filter related to given value") {

        val (fieldName, fieldValue) = ("type", StringUtils.singleQuoted("LOAN"))
        createPartition(None, fieldName, fieldValue, Seq.empty).facetFilter shouldBe s"$fieldName eq $fieldValue"
      }

      describe("retrieve documents matching") {
        it("only facet value") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(None, facetField, john, Seq.empty),
            stringValueEqJohn
          )
        }

        it("both filter and facet value") {

          val expectedPredicate: PushdownBean => Boolean = p =>
            stringValueEqJohn(p) &&
              p.intValue.exists(_.equals(1))

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(Some("intValue eq 1"), facetField, john, Seq.empty),
            expectedPredicate
          )
        }

        it("both filter and facet value and pushed predicate") {

          val dateValueNotNull: ODataExpression = new ODataExpression {
            override def toUriLiteral: String = "dateValue ne null"
          }

          val expectedPredicate: PushdownBean => Boolean = p =>
            stringValueEqJohn(p) &&
              p.intValue.exists(_.equals(1)) &&
              p.dateValue.isDefined

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(Some("intValue eq 1"), facetField, john, Seq(dateValueNotNull)),
            expectedPredicate
          )
        }
      }
    }
  }
}
