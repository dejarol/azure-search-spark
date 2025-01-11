package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.SearchDocument
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.models._
import com.github.jarol.azure.search.spark.sql.connector.{SearchITSpec, SearchTestUtils}

/**
 * Trait for integration tests related to [[SearchPartition]](s)
 */

trait SearchPartitionITSPec
  extends SearchITSpec {

  /**
   * Assert that a [[SearchPartition]] retrieves the proper set of documents
   * @param documents input documents
   * @param index target Search index
   * @param partition partition
   * @param expectedPredicate predicate for computing the expected set of documents
   * @tparam T document type (should extend [[ITDocument]])
   */

  protected final def assertCountPerPartition[T <: ITDocument](
                                                                documents: Seq[T],
                                                                index: String,
                                                                partition: SearchPartition,
                                                                expectedPredicate: T => Boolean
                                                              ): Unit = {

    // Write documents
    val expected: Seq[T] = documents.filter(expectedPredicate)

    // Retrieve matching documents
    val actual: Seq[SearchDocument] = JavaScalaConverters.listToSeq(
      SearchTestUtils.getPartitionDocuments(
        partition,
        getSearchClient(index)
      )
    )

    // Assertions: same size, same set of document ids
    actual should have size expected.size
    actual.map(_.get("id").asInstanceOf[String]) should contain theSameElementsAs expected.map(_.id)
  }
}
