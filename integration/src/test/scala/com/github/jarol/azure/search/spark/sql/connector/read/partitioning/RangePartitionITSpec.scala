package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.{DocumentIDGetter, DocumentSerializer}
import com.github.jarol.azure.search.spark.sql.connector.models._

class RangePartitionITSpec
  extends SearchPartitionITSPec {

  private lazy val (indexName, partitionField) = ("range-partition-it-spec", "value")
  private lazy val (lowerBound, upperBound) = (2, 5)

  private implicit lazy val serializer: DocumentSerializer[PairBean[Int]] = PairBean.serializerFor[Int]
  private implicit lazy val idGetter: DocumentIDGetter[PairBean[Int]] = idGetterFor()

  override def beforeAll(): Unit = {

    // Clean up and create index
    super.beforeAll()
    createIndexFromSchemaOf[PairBean[Int]](indexName)
  }

  /**
   * Create a collection of documents
   * @param values document values
   * @return a collection of documents
   */

  private def createDocuments(values: Option[Int]*): Seq[PairBean[Int]] = {

    values.zipWithIndex.map {
      case (value, index) => PairBean(
        String.valueOf(index),
        value
      )
    }
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
                               upper: Option[Int]
                             ): SearchPartition = {

    RangePartition(
      0,
      inputFilter,
      None,
      partitionField,
      lower.map(String.valueOf),
      upper.map(String.valueOf)
    )
  }

  describe(anInstanceOf[RangePartition]) {
    describe(SHOULD) {
      describe("retrieve documents whose field value") {
        it("is less than given upper bound or null") {

          val documents: Seq[PairBean[Int]] = createDocuments(
            None,
            Some(upperBound - 1),
            Some(upperBound),
            Some(upperBound + 1)
          )

          // Assertion
          val lessThanUpperBoundOrNull: PairBean[Int] => Boolean = _.value match {
            case Some(value) => value < upperBound
            case None => true
          }

          assertCountPerPartition(
            documents,
            indexName,
            createPartition(None, None, Some(upperBound)),
            lessThanUpperBoundOrNull
          )
        }

        it("is greater or equal than given lower bound") {

          truncateIndex(indexName)
          val documents: Seq[PairBean[Int]] = createDocuments(
            None,
            Some(lowerBound - 1),
            Some(lowerBound),
            Some(lowerBound + 1)
          )

          // Assertion
          val greaterOrEqualLowerBound: PairBean[Int] => Boolean = _.value.exists {
            _ >= lowerBound
          }

          assertCountPerPartition(
            documents,
            indexName,
            createPartition(None, Some(lowerBound), None),
            greaterOrEqualLowerBound
          )
        }

        it("falls within given range") {

          truncateIndex(indexName)
          val documents: Seq[PairBean[Int]] = createDocuments(
            None,
            Some(lowerBound - 1),
            Some(lowerBound),
            Some(lowerBound + 1),
            Some(upperBound - 1),
            Some(upperBound),
            Some(upperBound + 1)
          )

          // Assertion
          val inRange: PairBean[Int] => Boolean = _.value.exists {
            v => v >= lowerBound && v < upperBound
          }

          assertCountPerPartition(
            documents,
            indexName,
            createPartition(None, Some(lowerBound), Some(upperBound)),
            inRange
          )
        }
      }

      describe("combine input filter with") {
        it("only upper bound filter") {

        }

        it("only lower bound filter") {

        }

        it("range filter") {

        }
      }
    }
  }
}
