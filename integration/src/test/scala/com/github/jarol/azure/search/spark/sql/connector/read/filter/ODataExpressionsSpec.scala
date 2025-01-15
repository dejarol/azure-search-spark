package com.github.jarol.azure.search.spark.sql.connector.read.filter

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.models.PushdownBean
import com.github.jarol.azure.search.spark.sql.connector.{SearchITSpec, SearchTestUtils}

import java.time.LocalDate
import java.util.{List => JList}

class ODataExpressionsSpec
  extends SearchITSpec
    with ODataExpressionFactory {

  private lazy val now = LocalDate.now()
  private lazy val indexName = "odata-expressions-spec"
  private lazy val documents: Seq[PushdownBean] = Seq(
    PushdownBean(Some("one"), Some(1), Some(now)),
    PushdownBean(Some("two"), Some(2), Some(now.minusDays(1))),
    PushdownBean(Some("three"), None, Some(now.plusDays(1))),
    PushdownBean(Some("one"), Some(3), Some(now.plusDays(2))),
    PushdownBean(None, Some(4), Some(now)),
    PushdownBean(None, Some(4), Some(now.plusDays(3))),
    PushdownBean(Some("two"), Some(6), None),
    PushdownBean(Some("three"), Some(5), None)
  )

  private lazy val stringValue = topLevelFieldReference("stringValue")
  private lazy val intValue = topLevelFieldReference("intValue")
  private lazy val dateValue  = topLevelFieldReference("dateValue")

  override def beforeAll(): Unit = {

    super.beforeAll()
    createIndexFromSchemaOf[PushdownBean](indexName)
    writeDocuments[PushdownBean](indexName, documents)
  }

  /**
   * Retrieve document from an index, filtering documents according to the filter provided by a [[ODataExpression]] instance
   * @param expression adapter instance (will provide the OData filter string)
   * @return indexed documents that match the OData filter provided by the adapter
   */

  protected final def assertExpressionBehavior(
                                                expression: ODataExpression,
                                                predicate: PushdownBean => Boolean
                                              ): Unit = {

    val actualDocuments: JList[SearchDocument] = SearchTestUtils.readDocuments(
      getSearchClient(indexName),
      new SearchOptions().setFilter(
        expression.toUriLiteral
      )
    )

    val expectedDocuments: Seq[PushdownBean] = documents.filter(predicate)
    actualDocuments should have size expectedDocuments.size
    val expectedIds: Seq[String] = expectedDocuments.map(_.id)
    val actualIds: Seq[String] = JavaScalaConverters.listToSeq(actualDocuments).map {
      _.get("id").asInstanceOf[String]
    }

    actualIds should contain theSameElementsAs expectedIds
  }

  describe(`object`[ODataExpressions.type ]) {
    describe(SHOULD) {
      describe("create OData expressions for") {
        it("null equality conditions") {

          // IS_NULL
          assertExpressionBehavior(
            ODataExpressions.isNull(stringValue, negate = false),
            _.stringValue.isEmpty
          )

          // IS_NOT_NULL
          assertExpressionBehavior(
            ODataExpressions.isNull(stringValue, negate = true),
            _.stringValue.isDefined
          )
        }

        describe("comparisons for") {

          it("strings") {

            // EQUAL
            assertExpressionBehavior(
              ODataExpressions.comparison(stringValue, createStringLiteral("one"), ODataComparator.EQ),
              _.stringValue.exists(_.equals("one"))
            )
          }

          it("numbers") {

            // EQUAL
            assertExpressionBehavior(
              ODataExpressions.comparison(intValue, createIntLiteral(4), ODataComparator.EQ),
              _.intValue.exists(_.equals(4))
            )

            // NOT_EQUAL
            assertExpressionBehavior(
              ODataExpressions.comparison(intValue, createIntLiteral(1), ODataComparator.NE),
              _.intValue.forall {
                v => !v.equals(1)
              }
            )

            // GREATER
            assertExpressionBehavior(
              ODataExpressions.comparison(intValue, createIntLiteral(2), ODataComparator.GT),
              _.intValue.exists(_ > 2)
            )

            // GREATER_EQUAL
            assertExpressionBehavior(
              ODataExpressions.comparison(intValue, createIntLiteral(2), ODataComparator.GEQ),
              _.intValue.exists(_ >= 2)
            )

            // LESS
            assertExpressionBehavior(
              ODataExpressions.comparison(intValue, createIntLiteral(2), ODataComparator.LT),
              _.intValue.exists(_ < 2)
            )

            // LESS_EQUAL
            assertExpressionBehavior(
              ODataExpressions.comparison(intValue, createIntLiteral(2), ODataComparator.LEQ),
              _.intValue.exists(_ <= 2)
            )
          }

          it("dates") {

            // EQUAL
            assertExpressionBehavior(
              ODataExpressions.comparison(dateValue, createDateLiteral(now), ODataComparator.EQ),
              _.dateAsLocalDate.exists(_.equals(now))
            )

            // NOT_EQUAL
            assertExpressionBehavior(
              ODataExpressions.comparison(dateValue, createDateLiteral(now), ODataComparator.NE),
              _.dateAsLocalDate.forall(v => !v.equals(now))
            )

            // GREATER
            assertExpressionBehavior(
              ODataExpressions.comparison(dateValue, createDateLiteral(now), ODataComparator.GT),
              _.dateAsLocalDate.exists(_.isAfter(now))
            )

            // GREATER_EQUAL
            assertExpressionBehavior(
              ODataExpressions.comparison(dateValue, createDateLiteral(now), ODataComparator.GEQ),
              _.dateAsLocalDate.exists {
                d => d.equals(now) || d.isAfter(now)
              }
            )

            // LESS
            assertExpressionBehavior(
              ODataExpressions.comparison(dateValue, createDateLiteral(now), ODataComparator.LT),
              _.dateAsLocalDate.exists(_.isBefore(now))
            )

            // LESS_EQUAL
            assertExpressionBehavior(
              ODataExpressions.comparison(dateValue, createDateLiteral(now), ODataComparator.LEQ),
              _.dateAsLocalDate.exists {
                d => d.equals(now) || d.isBefore(now)
              }
            )
          }
        }

        it("negating an expression") {

          val value = "one"
          assertExpressionBehavior(
            ODataExpressions.not(
              ODataExpressions.comparison(
                stringValue, createStringLiteral(value), ODataComparator.EQ
              )
            ),
            _.stringValue.forall {
              v => !v.equals(value)
            }
          )
        }

        it("SQL-style IN conditions for strings") {

          val stringValues = Seq("one", "two")
          assertExpressionBehavior(
            ODataExpressions.in(
              stringValue,
              stringValues.map(createStringLiteral),
              ","
            ),
            _.stringValue.exists(stringValues.contains)
          )
        }

        it("logically combine other expressions") {

          // AND
          // TODO: test with only one expression
          val stringValueNotNull = ODataExpressions.isNull(stringValue, negate = true)
          val intValueEqTwo = ODataExpressions.comparison(intValue, createIntLiteral(2), ODataComparator.EQ)

          assertExpressionBehavior(
            ODataExpressions.logical(
              Seq(stringValueNotNull, intValueEqTwo),
              isAnd = true
            ),
            b => b.stringValue.isDefined && b.intValue.exists(_.equals(2))
          )

          // OR
          // TODO: test with only one expression
          assertExpressionBehavior(
            ODataExpressions.logical(
              Seq(stringValueNotNull, intValueEqTwo),
              isAnd = false
            ),
            b => b.stringValue.isDefined || b.intValue.exists(_.equals(2))
          )
        }
      }
    }
  }
}
