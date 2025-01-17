package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.models._

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
   * @param lowerBound lower bound
   * @param upperBound upper bound
   * @return
   */

  private def getSearchFilter(
                               fieldName: String,
                               lowerBound: Option[String],
                               upperBound: Option[String]
                             ): String = {

    RangePartition(
      0,
      fieldName,
      lowerBound,
      upperBound
    ).getPartitionFilter
  }

  /**
   * Create a partition
   * @param lower lower bound
   * @param upper upper bound
   * @return a partition
   */

  private def createPartition(
                               lower: Option[Int],
                               upper: Option[Int]
                             ): SearchPartition = {

    RangePartition(
      0,
      partitionField,
      lower.map(String.valueOf),
      upper.map(String.valueOf)
    )
  }

  describe(`object`[RangePartition]) {
    describe(SHOULD) {
      it("create a collection of partitions") {

        val (fieldName, values) = ("type", Seq("1", "2", "3"))
        val partitions = RangePartition.createCollection(fieldName, values)
        partitions should have size(values.size + 1)
        val headFilter = partitions.head.getPartitionFilter
        headFilter should include (s"$fieldName lt ${values.head}")
        headFilter should include (s"$fieldName eq null")
        partitions.last.getPartitionFilter shouldBe s"$fieldName ge ${values.last}"
      }
    }
  }

  describe(anInstanceOf[RangePartition]) {
    describe(SHOULD) {
      it("generate a filter that combines the 2 sub filters") {

        val (fieldName, lb, ub) = ("type", "1", "3")
        getSearchFilter(fieldName, None, None) shouldBe null
        val secondFilter = getSearchFilter(fieldName, Some(lb), None)
        secondFilter should include (s"$fieldName ge $lb")

        val thirdFilter = getSearchFilter(fieldName, None, Some(ub))
        thirdFilter should include (s"$fieldName lt $ub")
        thirdFilter should include (s"$fieldName eq null")

        val fourthFilter = getSearchFilter(fieldName, Some(lb), Some(ub))
        fourthFilter should include (s"$fieldName ge $lb")
        fourthFilter should include (s"$fieldName lt $ub")
      }

      describe("retrieve documents whose field value") {
        it("is less than given upper bound or null") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(None, Some(upperBound)),
            lessThanUpperBoundOrNull
          )
        }

        it("is greater or equal than given lower bound") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(Some(lowerBound), None),
            greaterOrEqualLowerBound
          )
        }

        it("falls within given range") {

          assertCountPerPartition[PushdownBean](
            documents,
            indexName,
            createPartition(Some(lowerBound), Some(upperBound)),
            inRange
          )
        }
      }
    }
  }
}
