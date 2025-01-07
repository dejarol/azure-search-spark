package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.read.filter.V2ExpressionAdapter

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
                                           predicates: Array[V2ExpressionAdapter]
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
          val thirdIsNotNullPredicate = new V2ExpressionAdapter {
            override def getODataExpression: String = s"$thirdField ne null"
          }
          val fourthIsNotNullPredicate = new V2ExpressionAdapter {
            override def getODataExpression: String = s"$fourthField ne null"
          }

          // Assert behavior without predicates
          createPartitionAndGetFilter(None, None, Array.empty) shouldBe null
          createPartitionAndGetFilter(Some(first), None, Array.empty) shouldBe first
          createPartitionAndGetFilter(None, Some(first), Array.empty) shouldBe first

          // Assert behavior with only predicates
          createPartitionAndGetFilter(None, None, Array(thirdIsNotNullPredicate)) shouldBe thirdIsNotNullPredicate.getODataExpression
          createPartitionAndGetFilter(None, None, Array(thirdIsNotNullPredicate, fourthIsNotNullPredicate)) shouldBe
            s"(${thirdIsNotNullPredicate.getODataExpression}) and (${fourthIsNotNullPredicate.getODataExpression})"

          // Assert behavior when combining
          createPartitionAndGetFilter(Some(first), Some(second), Array.empty) shouldBe s"($first) and ($second)"
          createPartitionAndGetFilter(Some(first), Some(second), Array(thirdIsNotNullPredicate)) shouldBe
            s"($first) and ($second) and (${thirdIsNotNullPredicate.getODataExpression})"
        }
      }
    }
  }
}
