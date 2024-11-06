package com.github.jarol.azure.search.spark.sql.connector

import com.azure.search.documents.indexes.models.SearchIndex
import com.github.jarol.azure.search.spark.sql.connector.core.JavaScalaConverters
import com.github.jarol.azure.search.spark.sql.connector.core.schema.SchemaUtils
import com.github.jarol.azure.search.spark.sql.connector.models.AbstractITDocument
import org.apache.spark.sql.Encoders
import org.scalatest.BeforeAndAfterAll

import scala.reflect.runtime.universe.TypeTag

/**
 * Mix-in trait for Search-Spark integration tests
 */

trait SearchSparkIntegrationSpec
  extends SearchSpec
    with SparkSpec
    with BeforeAndAfterAll {

  /**
   * Clean up all created indexes, at spec start-up
   */

  override final def beforeAll(): Unit = {

    listIndexes().foreach {
      index => dropIndexIfExists(index, sleep = false)
    }

    super.beforeAll()
  }

  /**
   * Clean up all created indexes, at spec tear-down
   */

  override final def afterAll(): Unit = {

    listIndexes().foreach {
      index => dropIndexIfExists(index, sleep = false)
    }

    super.afterAll()
  }

  /**
   * Create an index from the schema of a document
   * @param indexName index name
   * @tparam T type of document (must extend [[AbstractITDocument]] and be a case class)
   */

  protected final def createIndexFromSchemaOf[T <: AbstractITDocument with Product: TypeTag](indexName: String): Unit = {

    // Define Search fields
    val searchFields = Encoders.product[T].schema.map {
      spf =>
        val sef = SchemaUtils.toSearchField(spf)
        if (sef.getName.equals("id")) {
          sef.setKey(true)
        } else sef
    }

    // Create index
    searchIndexClient.createIndex(
      new SearchIndex(
        indexName,
        JavaScalaConverters.seqToList(searchFields)
      )
    )

    // Wait for some seconds in order to ensure test consistency
    Thread.sleep(5000)
  }
}
