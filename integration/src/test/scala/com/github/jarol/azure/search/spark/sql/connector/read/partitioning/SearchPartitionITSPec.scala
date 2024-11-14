package com.github.jarol.azure.search.spark.sql.connector.read.partitioning

import com.azure.search.documents.SearchDocument
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.{DocumentIDGetter, DocumentSerializer, SearchSparkITSpec, SearchTestUtils}
import com.github.jarol.azure.search.spark.sql.connector.models._

/**
 * Trait for integration tests related to [[SearchPartition]](s)
 */

trait SearchPartitionITSPec
  extends SearchSparkITSpec {

  protected final val defaultIdGetter: DocumentIDGetter[SearchDocument] = (document: SearchDocument) => document.getProperty("id")

  /**
   * Assert that a [[SearchPartition]] retrieves the proper set of documents
   * @param documents input documents
   * @param index target Search index
   * @param partition partition
   * @param expectedPredicate predicate for computing the expected set of documents
   * @tparam T document type (should have both a [[DocumentSerializer]] and [[DocumentIDGetter]] in scope)
   */

  protected final def assertCountPerPartition[T: DocumentSerializer: DocumentIDGetter](
                                                                                        documents: Seq[T],
                                                                                        index: String,
                                                                                        partition: SearchPartition,
                                                                                        expectedPredicate: T => Boolean
                                                                                      ): Unit = {

    // Write documents
    writeDocuments(index, documents)
    val expected: Seq[T] = documents.filter(expectedPredicate)

    // Retrieve matching documents
    val actual: Seq[SearchDocument] = JavaScalaConverters.listToSeq(
      SearchTestUtils.getPartitionDocuments(
        partition,
        getSearchClient(index)
      )
    )

    // Assertions: same size, same set of document ids
    val idGetter = implicitly[DocumentIDGetter[T]]
    actual should have size expected.size
    actual.map(defaultIdGetter.getId) should contain theSameElementsAs expected.map(idGetter.getId)
  }

  /**
   * Delete documents from an index
   * @param index index name
   * @param documents document to delete
   * @tparam T document type (should have an implicit [[DocumentIDGetter]] in scope)
   */

  protected final def deletedDocuments[T: DocumentIDGetter](
                                                             index: String,
                                                             documents: Seq[T]
                                                           ): Unit = {

    SearchTestUtils.deleteDocuments(
      getSearchClient(index),
      JavaScalaConverters.seqToList(documents),
      implicitly[DocumentIDGetter[T]]
    )
  }
}
