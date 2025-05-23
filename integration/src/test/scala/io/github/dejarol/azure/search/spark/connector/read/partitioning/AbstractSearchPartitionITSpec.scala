package io.github.dejarol.azure.search.spark.connector.read.partitioning

import com.azure.search.documents.SearchDocument
import io.github.dejarol.azure.search.spark.connector.SearchITSpec
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.models.ITDocument
import io.github.dejarol.azure.search.spark.connector.utils.SearchTestClients

/**
 * Trait for integration tests related to [[SearchPartition]](s)
 */

trait AbstractSearchPartitionITSpec
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
      SearchTestClients.getPartitionDocuments(
        partition,
        getSearchClient(index)
      )
    )

    // Assertions: same size, same set of document ids
    actual should have size expected.size
    actual.map(_.get("id").asInstanceOf[String]) should contain theSameElementsAs expected.map(_.id)
  }
}
