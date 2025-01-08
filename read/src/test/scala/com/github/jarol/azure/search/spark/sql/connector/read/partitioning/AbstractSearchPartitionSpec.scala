package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.read.filter.ODataExpression

class AbstractSearchPartitionSpec
  extends BasicSpec {

  /**
   * Create a partition instance
   * @param inputFilter input (user-specified) filter
   * @param partFilter partition filter
   * @param predicates predicates to push down
   * @return a partition instance
   */

  private def createPartitionAndGetFilter(
                                           inputFilter: Option[String],
                                           partFilter: Option[String],
                                           predicates: Array[ODataExpression]
                                         ): String = {

    new AbstractSearchPartition(
      0, inputFilter, None, predicates
    ) {
      override protected[partitioning] def partitionFilter: Option[String] = partFilter
    }.getODataFilter
  }

  describe(anInstanceOf[AbstractSearchPartition]) {
    describe(SHOULD) {
      describe("create a filter that") {
        it("combines input filter, partition filter and predicate filter") {

          val (first, second, thirdField, fourthField) = ("a eq 1", "b eq 2", "c", "d")
          val thirdIsNotNullPredicate = new ODataExpression {
            override def toUriLiteral: String = s"$thirdField ne null"
          }
          val fourthIsNotNullPredicate = new ODataExpression {
            override def toUriLiteral: String = s"$fourthField ne null"
          }

          // Assert behavior without predicates
          createPartitionAndGetFilter(None, None, Array.empty) shouldBe null
          createPartitionAndGetFilter(Some(first), None, Array.empty) shouldBe first
          createPartitionAndGetFilter(None, Some(first), Array.empty) shouldBe first

          // Assert behavior with only predicates
          createPartitionAndGetFilter(None, None, Array(thirdIsNotNullPredicate)) shouldBe thirdIsNotNullPredicate.toUriLiteral
          createPartitionAndGetFilter(None, None, Array(thirdIsNotNullPredicate, fourthIsNotNullPredicate)) shouldBe
            s"(${thirdIsNotNullPredicate.toUriLiteral}) and (${fourthIsNotNullPredicate.toUriLiteral})"

          // Assert behavior when combining
          createPartitionAndGetFilter(Some(first), Some(second), Array.empty) shouldBe s"($first) and ($second)"
          createPartitionAndGetFilter(Some(first), Some(second), Array(thirdIsNotNullPredicate)) shouldBe
            s"($first) and ($second) and (${thirdIsNotNullPredicate.toUriLiteral})"
        }
      }
    }
  }

  describe(`object`[AbstractSearchPartition]) {
    describe(SHOULD) {
      it("create a combined OData filter") {

        val (first, second) = ("a eq 1", "b eq 2")
        AbstractSearchPartition.createODataFilter(Seq.empty) shouldBe empty
        AbstractSearchPartition.createODataFilter(Seq(first)) shouldBe Some(first)
        AbstractSearchPartition.createODataFilter(Seq(first, second)) shouldBe Some(s"($first) and ($second)")
      }
    }
  }
}
