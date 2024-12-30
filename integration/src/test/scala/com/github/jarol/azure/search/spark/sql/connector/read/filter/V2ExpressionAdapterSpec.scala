package com.github.jarol.azure.search.spark.sql.connector.read.filter

import com.azure.search.documents.SearchDocument
import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.{DocumentSerializer, SearchSparkITSpec, SearchTestUtils}
import com.github.jarol.azure.search.spark.sql.connector.models.AbstractITDocument

import java.util.{List => JList}
import scala.reflect.runtime.universe.TypeTag

/**
 * Parent class for integration tests that deal with pushed predicates adapters
 */

trait V2ExpressionAdapterSpec
  extends SearchSparkITSpec {

  protected final val indexName = this.getClass.getSimpleName.toLowerCase

  /**
   * Create an index and populate it with some test documents
   * @param index index name
   * @param documents documents to write
   * @param serializer document serializer
   * @tparam D document type
   */

  protected final def createAndPopulateIndex[D <: AbstractITDocument with Product: TypeTag](
                                                                                             index: String,
                                                                                             documents: Seq[D]
                                                                                           )
                                                                                           (
                                                                                             serializer: DocumentSerializer[D]
                                                                                           ): Unit = {

    createIndexFromSchemaOf[D](index)
    writeDocuments(index, documents)(serializer)
  }

  /**
   * Retrieve document from an index, filtering documents according to the filter provided by a [[V2ExpressionAdapter]] instance
 *
   * @param index index name
   * @param adapter adapter instance (will provide the OData filter string)
   * @return indexed documents that match the OData filter provided by the adapter
   */

  protected final def readDocumentsUsingV2Adapter(
                                                   index: String,
                                                   adapter: V2ExpressionAdapter
                                                 ): Seq[SearchDocument] = {

    val documents: JList[SearchDocument] = SearchTestUtils.readDocuments(
      getSearchClient(index),
      new SearchOptions().setFilter(
        adapter.getODataFilter
      )
    )

    JavaScalaConverters.listToSeq(documents)
  }

  /**
   * Assert that two set of documents have same size and contain the same documents
   * @param actual actual set of documents
   * @param expected expected set of documents
   */

  protected final def assertSameSizeAndIds(
                                            actual: Seq[SearchDocument],
                                            expected: Seq[AbstractITDocument]
                                          ): Unit = {

    actual should have size expected.size
    actual.map(_.get("id").asInstanceOf[String]) should contain theSameElementsAs expected.map(_.id)
  }
}
