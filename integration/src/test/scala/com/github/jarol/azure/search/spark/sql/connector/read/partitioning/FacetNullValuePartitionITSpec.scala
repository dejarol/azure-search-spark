package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.utils.StringUtils
import com.github.jarol.azure.search.spark.sql.connector.models._
import com.github.jarol.azure.search.spark.sql.connector.read.filter.{ODataExpression, ODataExpressions}

import java.time.LocalDate

class FacetNullValuePartitionITSpec
  extends SearchPartitionITSPec {

  private lazy val FACET_FIELD_IS_NULL = "facet field is null"
  private lazy val NOT_MATCHING_OTHER_VALUES = "does not match other facet values"
  private lazy val (john, jane) = ("john", "jane")
  private lazy val (indexName, facetField) = ("facet-null-value-spec", "stringValue")
  private lazy val stringValueIsNullOrNotEqualToJohn: PushdownBean => Boolean = _.stringValue.forall(v => !v.equals(john))
  private lazy val intValueNotNull: PushdownBean => Boolean = _.intValue.isDefined

  private lazy val documents: Seq[PushdownBean] = Seq(
    PushdownBean(None, Some(1), None),
    PushdownBean(Some(john), Some(1), None),
    PushdownBean(Some(john), None, None),
    PushdownBean(None, Some(2), None),
    PushdownBean(Some(jane), Some(2), None),
    PushdownBean(Some(jane), None, Some(LocalDate.now()))
  )

  override def beforeAll(): Unit = {

    // Clean up and create index
    super.beforeAll()
    createIndexFromSchemaOf[PushdownBean](indexName)
    writeDocuments(indexName, documents)
  }

  /**
   * Create a partition instance
   * @param inputFilter input filter
   * @param facets facets
   * @return a partition instance
   */

  private def createPartition(
                               inputFilter: Option[String],
                               facets: Seq[String],
                               pushedExpressions: Seq[ODataExpression]
                             ): SearchPartition = {

    FacetNullValuePartition(
      inputFilter,
      None,
      pushedExpressions,
      facetField,
      facets.map(StringUtils.singleQuoted)
    )
  }

  describe(anInstanceOf[FacetNullValuePartition]) {
    describe(SHOULD) {
      describe("retrieve documents") {

        it(s"whose $FACET_FIELD_IS_NULL or $NOT_MATCHING_OTHER_VALUES") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(None, Seq(john), Seq.empty),
            stringValueIsNullOrNotEqualToJohn
          )
        }

        describe("that match") {
          it("the input filter") {

            val expectedPredicate: PushdownBean => Boolean = p => stringValueIsNullOrNotEqualToJohn(p) && intValueNotNull(p)
            assertCountPerPartition[PushdownBean](
              documents,
              indexName,
              createPartition(Some("intValue ne null"), Seq(john), Seq.empty),
              expectedPredicate
            )
          }

          it("both input filter and pushed predicate") {

            val expectedPredicate: PushdownBean => Boolean = p => stringValueIsNullOrNotEqualToJohn(p) && intValueNotNull(p) && p.dateValue.isDefined
            assertCountPerPartition[PushdownBean](
              documents,
              indexName,
              createPartition(
                Some(s"intValue ne null"),
                Seq(john),
                Seq(
                  ODataExpressions.isNull(
                    ODataExpressions.fieldReference(Seq("dateValue")),
                    negate = true
                  )
                ),
              ),
              expectedPredicate
            )
          }
        }
      }
    }
  }
}
