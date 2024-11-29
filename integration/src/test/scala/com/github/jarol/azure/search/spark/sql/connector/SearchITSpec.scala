package com.github.jarol.azure.search.spark.sql.connector

import com.azure.core.credential.AzureKeyCredential
import com.azure.search.documents.SearchClient
import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import com.azure.search.documents.indexes.{SearchIndexClient, SearchIndexClientBuilder}
import com.azure.search.documents.models.SearchOptions
import com.github.jarol.azure.search.spark.sql.connector.core.config.IOConfig
import com.github.jarol.azure.search.spark.sql.connector.core.utils.SearchUtils
import com.github.jarol.azure.search.spark.sql.connector.core.{BasicSpec, FieldFactory, JavaScalaConverters}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.TypeTag

/**
 * Trait to mix in for integration tests that require the interaction with a Search service
 */

trait SearchITSpec
  extends BasicSpec
    with FieldFactory {

  protected final lazy val SEARCH_END_POINT = sys.env("AZURE_SEARCH_ENDPOINT")
  protected final lazy val SEARCH_API_KEY = sys.env("AZURE_SEARCH_API_KEY")
  protected final lazy val KEY_CREDENTIALS = new AzureKeyCredential(SEARCH_API_KEY)

  protected final lazy val searchIndexClient: SearchIndexClient = new SearchIndexClientBuilder()
      .endpoint(SEARCH_END_POINT)
      .credential(KEY_CREDENTIALS)
      .buildClient

  /**
   * Get the minimum set of options required for reading or writing to a Search index
   * @param name index name
   * @return minimum options for read/write operations
   */

  protected final def optionsForAuthAndIndex(name: String): Map[String, String] = {

    Map(
      IOConfig.END_POINT_CONFIG -> SEARCH_END_POINT,
      IOConfig.API_KEY_CONFIG -> SEARCH_API_KEY,
      IOConfig.INDEX_CONFIG -> name
    )
  }

  /**
   * Get a Search index
   * @param name index name
   * @return a [[SearchIndex]] instance
   */

  protected final def getSearchIndex(name: String): SearchIndex = searchIndexClient.getIndex(name)

  /**
   * Get a client for search documents within an index
   * @param name index name
   * @return a [[SearchClient]] instance
   */

  protected final def getSearchClient(name: String): SearchClient = searchIndexClient.getSearchClient(name)

  /**
   * Create an index with given name and fields
   * @param name name
   * @param fields fields
   */

  protected final def createIndex(
                                   name: String,
                                   fields: Seq[SearchField]
                                 ): Unit = {

    searchIndexClient.createIndex(
      new SearchIndex(
        name,
        JavaScalaConverters.seqToList(fields)
      )
    )
  }

  /**
   * Get the names of existing Search indexes
   * @return collection of existing indexes
   */

  protected final def listIndexes(): Seq[String] = {

    JavaScalaConverters.listToSeq(
      SearchTestUtils.listIndexes(searchIndexClient)
    )
  }

  /**
   * Evaluate if an index exists
   * @param name name
   * @return true for existing indexes
   */

  protected final def indexExists(name: String): Boolean = {

   SearchUtils.indexExists(
     searchIndexClient,
     name
   )
  }

  /**
   * Count the documents within an index (approximately)
   * @param name index name
   * @return approximate number of documents within an index
   */

  protected final def countDocumentsForIndex(name: String): Long = {

    SearchUtils.getSearchPagedIterable(
      getSearchClient(name),
      new SearchOptions().setIncludeTotalCount(true)
    ).getTotalCount
  }

  /**
   * Drop an index, if it exists
   * @param name name of the index to drop
   */

  protected final def dropIndexIfExists(
                                         name: String,
                                         sleep: Boolean
                                       ): Unit = {

    if (indexExists(name)) {
      searchIndexClient.deleteIndex(name)
      if (sleep) {
        Thread.sleep(10000)
      }
    }
  }

  /**
   * Get the list of field defined by an index
   * @param name index name
   * @return a collection with defined index fields
   */

  protected final def getIndexFields(name: String): Map[String, SearchField] = {

    CaseInsensitiveMap[SearchField](
      JavaScalaConverters.listToSeq(
        getSearchIndex(name).getFields
      ).map {
        field => (field.getName, field)
      }.toMap
    )
  }

  /**
   * Write a collection of documents to an index
   * @param indexName index name
   * @param documents documents
   * @tparam T document type (an implicit [[DocumentSerializer]] for this type is expected to be on scope)
   */

  protected final def writeDocuments[T: DocumentSerializer](
                                                             indexName: String,
                                                             documents: Seq[T]
                                                           ): Unit = {

    SearchTestUtils.writeDocuments[T](
      getSearchClient(indexName),
      JavaScalaConverters.seqToList(documents),
      implicitly[DocumentSerializer[T]]
    )

    // Wait for some seconds in order to ensure test consistency
    Thread.sleep(5000)
  }

  /**
   * Read documents from an index as collection of instances of a target type
   * @param index index name
   * @tparam T target type (should have an implicit [[DocumentDeserializer]] in scope)
   * @return a collection of typed documents
   */

  protected final def readDocumentsAs[T: DocumentDeserializer](index: String): Seq[T] = {

    val deserializer = implicitly[DocumentDeserializer[T]]
    JavaScalaConverters.listToSeq(
      SearchTestUtils.readDocuments(getSearchClient(index))
    ).map {
      deserializer.deserialize(_)
    }
  }

  /**
   * Assert that a Search index contains the same field names of a schema
   * @param schema expected schema
   * @param index Search index name
   */

  protected final def assertMatchBetweenSchemaAndIndex(
                                                        schema: StructType,
                                                        index: String
                                                      ): Unit = {


    val expectedFields = schema.map(_.name)
    val actualFieldsNames = getIndexFields(index).keySet

    // Assert same size and content
    actualFieldsNames should have size expectedFields.size
    actualFieldsNames should contain theSameElementsAs expectedFields
  }

  /**
   * Assert that a Search index contains the same fields of a case class
   * @param index Search index name
   * @tparam T type of expected matching case class
   */

  protected final def assertMatchBetweenSchemaAndIndex[T <: Product: TypeTag](index: String): Unit = {

    assertMatchBetweenSchemaAndIndex(
      Encoders.product[T].schema,
      index
    )
  }
}

