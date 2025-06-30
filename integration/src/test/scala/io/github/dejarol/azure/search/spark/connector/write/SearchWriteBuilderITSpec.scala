package io.github.dejarol.azure.search.spark.connector.write

import io.github.dejarol.azure.search.spark.connector.SearchITSpec
import io.github.dejarol.azure.search.spark.connector.write.config._
import org.apache.spark.sql.types.DataTypes
import org.scalatest.BeforeAndAfterEach

class SearchWriteBuilderITSpec
  extends SearchITSpec
    with WriteConfigFactory
      with BeforeAndAfterEach {

  private lazy val (idFieldName, testIndex) = ("id", "write-builder-index")
  private lazy val keyField = createStructField(idFieldName, DataTypes.StringType)
  private lazy val minimumOptionsForIndexCreation = optionsForAuthAndIndex(testIndex) + (
    fieldOptionKey(idFieldName) ->
      s"""
         |{
         |  "key": true
         |}
         |""".stripMargin
    )

  /**
   * Delete index used for integration testing
   */

  override def afterEach(): Unit = {

    dropIndexIfExists(testIndex, sleep = true)
    super.afterEach()
  }

  describe(anInstanceOf[SearchWriteBuilder]) {
    describe(SHOULD) {

      // Schemas for test execution
      lazy val previousSchema = createStructType(
        keyField,
        createStructField("name", DataTypes.StringType)
      )

      lazy val currentSchema = createStructType(
        keyField,
        createStructField("description", DataTypes.StringType),
        createStructField("createdDate", DataTypes.TimestampType
        )
      )

      it("truncate an existing index") {

        indexExists(testIndex) shouldBe false
        createIndexWithSchema(testIndex, previousSchema)
        indexExists(testIndex) shouldBe true
        assertMatchBetweenSchemaAndIndex(previousSchema, testIndex)

        // Trigger truncation and assert result
        val truncatingBuilder = new SearchWriteBuilder(
          WriteConfig(minimumOptionsForIndexCreation),
          currentSchema
        ).truncate()

        truncatingBuilder.buildForBatch()
        indexExists(testIndex) shouldBe true
        assertMatchBetweenSchemaAndIndex(currentSchema, testIndex)
      }

      it("leave an existing index as-is if truncation flag is disabled") {

        indexExists(testIndex) shouldBe false
        createIndexWithSchema(testIndex, previousSchema)
        indexExists(testIndex) shouldBe true
        assertMatchBetweenSchemaAndIndex(previousSchema, testIndex)

        // Trigger truncation and assert result
        val nonTruncatingBuilder = new SearchWriteBuilder(
          WriteConfig(minimumOptionsForIndexCreation),
          currentSchema,
          false
        )

        nonTruncatingBuilder.buildForBatch()
        indexExists(testIndex) shouldBe true
        assertMatchBetweenSchemaAndIndex(previousSchema, testIndex)
      }
    }
  }
}
