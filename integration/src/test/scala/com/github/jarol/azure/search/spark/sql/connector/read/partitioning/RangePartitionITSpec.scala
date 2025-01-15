package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.models._
import com.github.jarol.azure.search.spark.sql.connector.read.filter.ODataExpression

class RangePartitionITSpec
  extends AbstractSearchPartitionITSpec {

  private lazy val (lowerBound, upperBound) = (2, 5)
  private lazy val (indexName, partitionField) = ("range-partition-it-spec", "intValue")
  private lazy val documents: Seq[PushdownBean] = Seq(
    PushdownBean(Some("john"), None, None),
    PushdownBean(None, Some(lowerBound - 1), None),
    PushdownBean(Some("john"), Some(lowerBound), None),
    PushdownBean(None, Some(lowerBound + 1), None),
    PushdownBean(Some("john"), Some(upperBound - 1), None),
    PushdownBean(None, Some(upperBound), None),
    PushdownBean(None, Some(upperBound + 1), None)
  )

  private lazy val lessThanUpperBoundOrNull: PushdownBean => Boolean = _.intValue.forall(_ < upperBound)
  private lazy val greaterOrEqualLowerBound: PushdownBean => Boolean = _.intValue.exists(_ >= lowerBound)
  private lazy val inRange: PushdownBean => Boolean = _.intValue.exists {
    v => v >= lowerBound && v < upperBound
  }

  override def beforeAll(): Unit = {

    // Clean up and create index
    super.beforeAll()
    createIndexFromSchemaOf[PushdownBean](indexName)
    writeDocuments(indexName, documents)
  }

  /**
   * Create a partition instance
   * @param inputFilter input filter
   * @param lowerBound lower bound
   * @param upperBound upper bound
   * @return
   */

  private def getSearchFilter(
                               inputFilter: Option[String],
                               fieldName: String,
                               lowerBound: Option[String],
                               upperBound: Option[String]
                             ): String = {

    RangePartition(
      0,
      inputFilter,
      None,
      Seq.empty,
      fieldName,
      lowerBound,
      upperBound
    ).getODataFilter
  }

  /**
   * Create a partition
   * @param inputFilter input filter
   * @param lower lower bound
   * @param upper upper bound
   * @return a partition
   */

  private def createPartition(
                               inputFilter: Option[String],
                               lower: Option[Int],
                               upper: Option[Int],
                               pushedPredicates: Seq[ODataExpression]
                             ): SearchPartition = {

    RangePartition(
      0,
      inputFilter,
      None,
      pushedPredicates,
      partitionField,
      lower.map(String.valueOf),
      upper.map(String.valueOf)
    )
  }

  describe(`object`[RangePartition]) {
    describe(SHOULD) {
      it("create a collection of partitions") {

        val (fieldName, values) = ("type", Seq("1", "2", "3"))
        val partitions = RangePartition.createCollection(None, None, Seq.empty, fieldName, values)
        partitions should have size(values.size + 1)
        val headFilter = partitions.head.getODataFilter
        headFilter should include (s"$fieldName lt ${values.head}")
        headFilter should include (s"$fieldName eq null")
        partitions.last.getODataFilter shouldBe s"$fieldName ge ${values.last}"
      }
    }
  }

  describe(anInstanceOf[RangePartition]) {
    describe(SHOULD) {
      it("generate a filter that combines the 3 sub filters") {

        val (fieldName, inputFilter, lb, ub) = ("type", "name eq 'hello'", "1", "3")
        getSearchFilter(None, fieldName, None, None) shouldBe null
        getSearchFilter(Some(inputFilter), fieldName, None, None) shouldBe inputFilter
        val secondFilter = getSearchFilter(Some(inputFilter), fieldName, Some(lb), None)
        secondFilter should include (inputFilter)
        secondFilter should include (s"$fieldName ge $lb")

        val thirdFilter = getSearchFilter(None, fieldName, None, Some(ub))
        thirdFilter should include (s"$fieldName lt $ub")
        thirdFilter should include (s"$fieldName eq null")

        val fourthFilter = getSearchFilter(Some(inputFilter), fieldName, Some(lb), Some(ub))
        fourthFilter should include (inputFilter)
        fourthFilter should include (s"$fieldName ge $lb")
        fourthFilter should include (s"$fieldName lt $ub")
      }

      describe("retrieve documents whose field value") {
        it("is less than given upper bound or null") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(None, None, Some(upperBound), Seq.empty),
            lessThanUpperBoundOrNull
          )
        }

        it("is greater or equal than given lower bound") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(None, Some(lowerBound), None, Seq.empty),
            greaterOrEqualLowerBound
          )
        }

        it("falls within given range") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(None, Some(lowerBound), Some(upperBound), Seq.empty),
            inRange
          )
        }
      }

      describe("combine input filter with") {
        it("only upper bound filter") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(Some("stringValue ne null"), None, Some(upperBound), Seq.empty),
            p => p.stringValue.isDefined && lessThanUpperBoundOrNull(p)
          )
        }

        it("only lower bound filter") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(Some("stringValue ne null"), Some(lowerBound), None, Seq.empty),
            p => p.stringValue.isDefined && greaterOrEqualLowerBound(p)
          )
        }

        it("range filter") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(Some("stringValue ne null"), Some(lowerBound), Some(upperBound), Seq.empty),
            p => p.stringValue.isDefined && inRange(p)
          )
        }
      }
    }
  }
}
