package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.models._
import com.github.jarol.azure.search.spark.sql.connector.read.filter.ODataExpression

class RangePartitionITSpec
  extends SearchPartitionITSPec {

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

  override def beforeAll(): Unit = {

    // Clean up and create index
    super.beforeAll()
    createIndexFromSchemaOf[PushdownBean](indexName)
    writeDocuments(indexName, documents)
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

  describe(anInstanceOf[RangePartition]) {
    describe(SHOULD) {
      describe("retrieve documents whose field value") {
        it("is less than given upper bound or null") {

          // Assertion
          val lessThanUpperBoundOrNull: PushdownBean => Boolean = _.intValue.forall(_ < upperBound)
          assertCountPerPartition(
            documents,
            indexName,
            createPartition(None, None, Some(upperBound), Seq.empty),
            lessThanUpperBoundOrNull
          )
        }

        it("is greater or equal than given lower bound") {

          // Assertion
          val greaterOrEqualLowerBound: PushdownBean => Boolean = _.intValue.exists(_ >= lowerBound)
          assertCountPerPartition(
            documents,
            indexName,
            createPartition(None, Some(lowerBound), None, Seq.empty),
            greaterOrEqualLowerBound
          )
        }

        it("falls within given range") {

          // Assertion
          val inRange: PushdownBean => Boolean = _.intValue.exists {
            v => v >= lowerBound && v < upperBound
          }

          assertCountPerPartition(
            documents,
            indexName,
            createPartition(None, Some(lowerBound), Some(upperBound), Seq.empty),
            inRange
          )
        }
      }

      describe("combine input filter with") {
        it("only upper bound filter") {

          // TODO: add tests
        }

        it("only lower bound filter") {

          // TODO: add tests
        }

        it("range filter") {

          // TODO: add tests
        }
      }
    }
  }
}
