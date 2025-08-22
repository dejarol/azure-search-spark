package io.github.dejarol.azure.search.spark.connector

import com.azure.core.credential.AzureKeyCredential
import com.azure.search.documents.SearchClient
import com.azure.search.documents.indexes.models.{SearchField, SearchIndex}
import com.azure.search.documents.indexes.{SearchIndexClient, SearchIndexClientBuilder}
import io.github.dejarol.azure.search.spark.connector.core.JavaScalaConverters
import io.github.dejarol.azure.search.spark.connector.core.config.IOConfig
import io.github.dejarol.azure.search.spark.connector.core.schema.{NoOpFieldCreationContext, SchemaUtils}
import io.github.dejarol.azure.search.spark.connector.core.utils.SearchClients
import io.github.dejarol.azure.search.spark.connector.models.{DocumentDeserializer, DocumentSerializer, ITDocument}
import io.github.dejarol.azure.search.spark.connector.utils.SearchTestClients
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfterAll

import java.util.{List => JList}
import scala.reflect.runtime.universe.TypeTag

/**
 * Trait to mix in for integration tests that require interacting with a Search service
 */

trait SearchITSpec
  extends BasicSpec
    with FieldFactory
      with FieldAssertionMixins
        with BeforeAndAfterAll {

  protected final lazy val propertiesSupplier: IntegrationPropertiesSupplier = IntegrationPropertiesSuppliers.resolve()
  protected final lazy val searchIndexClient: SearchIndexClient = new SearchIndexClientBuilder()
      .endpoint(propertiesSupplier.endPoint())
      .credential(new AzureKeyCredential(propertiesSupplier.apiKey()))
      .buildClient

  protected final lazy val optionsForAuth = Map(
    IOConfig.END_POINT_CONFIG -> propertiesSupplier.endPoint(),
    IOConfig.API_KEY_CONFIG -> propertiesSupplier.apiKey()
  )

  /**
   * Clean up all created indexes, at spec start-up
   */

  override def beforeAll(): Unit = {

    super.beforeAll()
    listIndexes().foreach {
      index => dropIndexIfExists(index, sleep = false)
    }
  }

  /**
   * Clean up all created indexes, at spec tear-down
   */

  override final def afterAll(): Unit = {

    super.afterAll()
    listIndexes().foreach {
      index => dropIndexIfExists(index, sleep = false)
    }
  }

  /**
   * Get the minimum set of options required for reading or writing to a Search index
   * @param name index name
   * @return minimum options for read/write operations
   */

  protected final def optionsForAuthAndIndex(name: String): Map[String, String] = {

    optionsForAuth + (
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
   * Create an index with given name and Spark schema
   * @param name index name
   * @param schema Spark schema
   * @since 0.11.0
   */

  protected final def createIndexWithSchema(
                                             name: String,
                                             schema: StructType
                                           ): Unit = {

    // Define Search fields
    val searchFields = schema.map {
      structField =>
        val searchField = SchemaUtils.toSearchField(structField, NoOpFieldCreationContext)
        if (searchField.getName.equals("id")) {
          searchField.setKey(true)
        } else searchField
    }

    createIndex(name, searchFields)

    // Wait for some seconds in order to ensure test consistency
    Thread.sleep(3000)
  }


  /**
   * Create an index from the schema of a document that extends [[ITDocument]]
   * @param indexName index name
   * @tparam T type of document (must extend [[ITDocument]] and be a case class)
   */

  protected final def createIndexFromSchemaOf[T <: ITDocument with Product: TypeTag](indexName: String): Unit = {

    createIndexWithSchema(
      indexName,
      Encoders.product[T].schema
    )
  }

  /**
   * Get the names of existing Search indexes
   * @return collection of existing indexes
   */

  protected final def listIndexes(): Seq[String] = {

    JavaScalaConverters.listToSeq(
      SearchTestClients.listIndexes(searchIndexClient)
    )
  }

  /**
   * Evaluate if an index exists
   * @param name name
   * @return true for existing indexes
   */

  protected final def indexExists(name: String): Boolean = SearchClients.indexExists(searchIndexClient, name)

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
        Thread.sleep(3000)
      }
    }
  }

  protected final def getIndexFieldsAsMap(fields: JList[SearchField]): Map[String, SearchField] = {

    CaseInsensitiveMap[SearchField](
      JavaScalaConverters.listToSeq(fields).map {
        field => (field.getName, field)
      }.toMap
    )
  }

  /**
   * Get the list of field defined by an index
   * @param indexName index name
   * @return a collection with defined index fields
   */

  protected final def getIndexFieldsAsMap(indexName: String): Map[String, SearchField] = {

    getIndexFieldsAsMap(
      getSearchIndex(indexName).getFields
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

    SearchTestClients.writeDocuments[T](
      getSearchClient(indexName),
      JavaScalaConverters.seqToList(documents),
      implicitly[DocumentSerializer[T]]
    )

    // Wait for some seconds in order to ensure test consistency
    Thread.sleep(3000)
  }

  /**
   * Read documents from an index as collection of instances of a target type
   * @param index index name
   * @tparam T target type (should have an implicit [[DocumentDeserializer]] in scope)
   * @return a collection of typed documents
   */

  protected final def readAllDocumentsAs[T: DocumentDeserializer](index: String): Seq[T] = {

    val deserializer = implicitly[DocumentDeserializer[T]]
    JavaScalaConverters.listToSeq(
      SearchTestClients.readAllDocuments(getSearchClient(index))
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


    val expectedFields = schema.map(_.name.toLowerCase)
    val actualFieldsNames = getIndexFieldsAsMap(index).keySet.map(_.toLowerCase)

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

