package com.github.jarol.azure.search.spark.sql.connector.read.filter

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.models.PushdownBean
import com.github.jarol.azure.search.spark.sql.connector.{SearchSparkITSpec, SearchTestUtils}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

import java.time.LocalDate
import java.util.{List => JList}

class ODataExpressionsSpec
  extends SearchSparkITSpec {

  private lazy val now = LocalDate.now()
  private lazy val indexName = "odata-expressions-spec"
  private lazy val documents: Seq[PushdownBean] = Seq(
    PushdownBean(Some("one"), Some(1), Some(now)),
    PushdownBean(Some("two"), Some(2), Some(now.minusDays(1))),
    PushdownBean(Some("three"), None, Some(now.plusDays(1))),
    PushdownBean(Some("one"), Some(3), Some(now.plusDays(2))),
    PushdownBean(None, Some(4), Some(now)),
    PushdownBean(None, Some(4), Some(now.plusDays(3)))
  )

  private lazy val stringValue = topLevelField("stringValue")
  private lazy val intValue = topLevelField("intValue")
  private lazy val dateValue  = topLevelField("dateValue")

  /**
   * Create an expression for a top-level field
   * @param name field name
   * @return an expression for a top-level field
   */

  private def topLevelField(name: String): ODataExpression = ODataExpressions.fieldReference(Seq(name))

  private def createStringLiteral(string: String): ODataExpression = ODataExpressions.literal(DataTypes.StringType, UTF8String.fromString(string))

  private def createIntLiteral(int: Integer): ODataExpression = ODataExpressions.literal(DataTypes.IntegerType, int)

  private def createDateLiteral(date: LocalDate): ODataExpression = ODataExpressions.literal(DataTypes.DateType, date.toEpochDay.toInt)

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
      describe("create OData expressions") {
        it("for null equality checks") {

          createIndexFromSchemaOf[PushdownBean](indexName)
          writeDocuments[PushdownBean](indexName, documents)

          // IS_NULL
          assertExpressionBehavior(
            ODataExpressions.isNull(topLevelField("stringValue"), negate = false),
            _.stringValue.isEmpty
          )

          // IS_NOT_NULL
          assertExpressionBehavior(
            ODataExpressions.isNull(topLevelField("stringValue"), negate = true),
            _.stringValue.isDefined
          )
        }

        it("for comparisons") {

          // [a] EQUAL
          // [a.1] equal: string
          assertExpressionBehavior(
            ODataExpressions.comparison(stringValue, createStringLiteral("one"), ODataComparator.EQ),
            _.stringValue.exists(_.equals("one"))
          )

          // [a.2] equal: int
          assertExpressionBehavior(
            ODataExpressions.comparison(intValue, createIntLiteral(4), ODataComparator.EQ),
            _.intValue.exists(_.equals(4))
          )

          // [a.3] equal: date
          assertExpressionBehavior(
            ODataExpressions.comparison(dateValue, createDateLiteral(now), ODataComparator.EQ),
            _.dateValue.exists(_.toLocalDateTime.toLocalDate.equals(now))
          )

          // [b] NOT_EQUAL
          assertExpressionBehavior(
            ODataExpressions.comparison(intValue, createIntLiteral(1), ODataComparator.NE),
            _.intValue.forall {
              v => !v.equals(1)
            }
          )

          // [c] GREATER
          // [c.1] greater: int
          assertExpressionBehavior(
            ODataExpressions.comparison(intValue, createIntLiteral(2), ODataComparator.GT),
            _.intValue.exists(_ > 2)
          )

          // [c.2] greater: date
          // TODO

          // [d] GREATER_EQUAL
          // [d.1] geq: int
          assertExpressionBehavior(
            ODataExpressions.comparison(intValue, createIntLiteral(2), ODataComparator.GEQ),
            _.intValue.exists(_ >= 2)
          )

          // [d.2] geq: date
          // TODO

          // [e] LESS
          // [e.1] lt: int
          assertExpressionBehavior(
            ODataExpressions.comparison(intValue, createIntLiteral(2), ODataComparator.LT),
            _.intValue.exists(_ < 2)
          )

          // [e.2] lt: date
          // TODO

          // [f] LESS_EQUAL
          // [f.1] leq: int
          assertExpressionBehavior(
            ODataExpressions.comparison(intValue, createIntLiteral(2), ODataComparator.LEQ),
            _.intValue.exists(_ <= 2)
          )

          // [f.2] leq: date
          // TODO
        }
      }
    }
  }
}
