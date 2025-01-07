package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.github.jarol.azure.search.spark.sql.connector.core.BasicSpec
import com.github.jarol.azure.search.spark.sql.connector.read.V2PredicateFactory
import com.github.jarol.azure.search.spark.sql.connector.read.filter.V2ExpressionAdapterFactory
import org.apache.spark.sql.connector.expressions.filter.Predicate

class AbstractSearchPartitionSpec
  extends BasicSpec
    with V2PredicateFactory {

  /**
   * Retrieve the OData filter related to given predicate, invoking [[V2ExpressionAdapterFactory.build]]
 *
   * @param predicate predicate to parse
   * @return the OData filter for given predicate
   */

  private def getPredicateFilter(predicate: Predicate): String = {

    val maybeResult = V2ExpressionAdapterFactory.build(predicate)
    maybeResult shouldBe defined
    maybeResult.get
  }

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
                                           predicates: Array[Predicate]
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
          val thirdIsNotNullPredicate = createNullEqualityPredicate(thirdField, negate = true)
          val fourthIsNotNullPredicate = createNullEqualityPredicate(fourthField, negate = false)

          // Assert behavior without predicates
          createPartitionAndGetFilter(None, None, Array.empty) shouldBe null
          createPartitionAndGetFilter(Some(first), None, Array.empty) shouldBe first
          createPartitionAndGetFilter(None, Some(first), Array.empty) shouldBe first

          // Assert behavior with only predicates
          createPartitionAndGetFilter(None, None, Array(thirdIsNotNullPredicate)) shouldBe getPredicateFilter(thirdIsNotNullPredicate)
          createPartitionAndGetFilter(None, None, Array(thirdIsNotNullPredicate, fourthIsNotNullPredicate)) shouldBe
            s"(${getPredicateFilter(thirdIsNotNullPredicate)}) and (${getPredicateFilter(fourthIsNotNullPredicate)})"

          // Assert behavior when combining
          createPartitionAndGetFilter(Some(first), Some(second), Array.empty) shouldBe s"($first) and ($second)"
          createPartitionAndGetFilter(Some(first), Some(second), Array(thirdIsNotNullPredicate)) shouldBe
            s"($first) and ($second) and (${getPredicateFilter(thirdIsNotNullPredicate)})"
        }
      }
    }
  }
}
