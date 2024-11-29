package com.github.jarol.azure.search.spark.sql.connector.write

import com.azure.search.documents.indexes.models.{LexicalAnalyzerName, SearchField}
import com.github.jarol.azure.search.spark.sql.connector.SearchITSpec
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.scalatest.BeforeAndAfterEach

class SearchWriteBuilderITSpec
  extends SearchITSpec
    with BeforeAndAfterEach {

  private lazy val idFieldName = "id"
  private lazy val testIndex = "write-builder-index"
  private lazy val keyField = createStructField(idFieldName, DataTypes.StringType)
  private lazy val analyzer = LexicalAnalyzerName.STANDARD_ASCII_FOLDING_LUCENE
  private lazy val minimumOptionsForIndexCreation = optionsForAuthAndIndex(testIndex) + (
    WriteConfig.FIELD_OPTIONS_PREFIX + WriteConfig.KEY_FIELD_CONFIG -> idFieldName
    )

  /**
   * Delete index used for integration testing
   */

  override def afterEach(): Unit = {

    dropIndexIfExists(testIndex, sleep = true)
    super.afterEach()
  }

  /**
   * Safely create an index
   * @param schema schema
   * @param options options
   */

  private def safelyCreateIndex(
                                 schema: StructType,
                                 options: Map[String, String]
                               ): Unit = {

    // Take options for auth and index,
    // add key field and provided options
    SearchWriteBuilder.safelyCreateIndex(
      WriteConfig(minimumOptionsForIndexCreation ++ options),
      schema
    )

    Thread.sleep(5000)
  }

  /**
   * Assert that a field has been properly enabled/disabled when creating a new index
   * @param schema schema for index creation
   * @param fieldList name of fields to enable
   * @param asserter feature asserter
   */

  private def assertFeatureDisabling(
                                      schema: StructType,
                                      fieldList: Seq[String],
                                      asserter: FeatureAsserter
                                   ): Unit = {

    // Create index
    indexExists(testIndex) shouldBe false
    safelyCreateIndex(schema, Map(WriteConfig.FIELD_OPTIONS_PREFIX + asserter.suffix -> fieldList.mkString(",")))
    indexExists(testIndex) shouldBe true

    // Retrieve index fields
    val matchingFields = getIndexFields(testIndex).collect {
      case (k, v) if fieldList.exists {
        _.equalsIgnoreCase(k)
      } => v
    }

    // Assertion for matching fields
    forAll(matchingFields) {
      field =>
        if (asserter.refersToDisablingFeature) {
          asserter.getFeatureValue(field) shouldBe Some(false)
        } else {
          asserter.getFeatureValue(field) shouldBe Some(true)
        }
    }
  }

  /**
   * Create a Search index, setting some field analyzers, and get back the list of generated Search fields
   * @param schema schema
   * @param analyzerType analyzer type
   * @param onFields map with keys being analyzers aliases and values being list of fields on which setting the analyzer
   * @return fields from the newly created Search index
   */

  private def createIndexSettingAnalyzers(
                                           schema: StructType,
                                           analyzerType: SearchFieldAnalyzerType,
                                           onFields: Map[String, Seq[String]]
                                         ): Map[String, SearchField] = {

    indexExists(testIndex) shouldBe false
    safelyCreateIndex(
      schema,
      analyzerType.rawConfigForAnalyzers(
        analyzer,
        onFields
      )
    )

    indexExists(testIndex) shouldBe true
    getIndexFields(testIndex)
  }

  describe(`object`[SearchWriteBuilder]) {
    describe(SHOULD) {
      describe("create an index") {
        it("with as many fields as many columns") {

          val schema = createStructType(
            keyField,
            createStructField("name", DataTypes.StringType),
            createStructField("date", DataTypes.TimestampType),
            createArrayField("education",
              createStructType(
                createStructField("city", DataTypes.StringType),
                createStructField("title", DataTypes.StringType),
                createStructField("grade", DataTypes.IntegerType)
              )
            )
          )

          indexExists(testIndex) shouldBe false
          safelyCreateIndex(schema, Map.empty)
          indexExists(testIndex) shouldBe true
          assertMatchBetweenSchemaAndIndex(schema, testIndex)
        }

        it("not including the column used for index action type") {

          val actionTypeColName = "actionType"
          val schema = createStructType(
            keyField,
            createStructField("value", DataTypes.LongType),
            createStructField(actionTypeColName, DataTypes.StringType)
          )

          indexExists(testIndex) shouldBe false
          safelyCreateIndex(
            schema,
            Map(
              WriteConfig.INDEX_ACTION_COLUMN_CONFIG -> actionTypeColName
            )
          )
          indexExists(testIndex) shouldBe true
          val actualFields = getIndexFields(testIndex)

          val expectedSchema = schema.filterNot {
            _.name.equalsIgnoreCase(actionTypeColName)
          }

          actualFields should have size expectedSchema.size
          actualFields.keySet should contain theSameElementsAs expectedSchema.map(_.name)
        }

        describe("enriching fields with") {
          describe("some features, like") {
            it("facetable") {

              val nonFacetableField = createStructField("category", DataTypes.StringType)
              val schema = createStructType(
                keyField,
                createStructField("discount", DataTypes.DoubleType),
                nonFacetableField
              )

              assertFeatureDisabling(schema, Seq(nonFacetableField.name), FeatureAsserter.FACETABLE)
            }

            it("filterable") {

              val nonFilterableField = createStructField("level", DataTypes.IntegerType)
              val schema = createStructType(
                keyField,
                nonFilterableField,
                createStructField("date", DataTypes.TimestampType)
              )

              assertFeatureDisabling(schema, Seq(nonFilterableField.name), FeatureAsserter.FILTERABLE)
            }

            it("hidden") {

              val firstHidden = createStructField("first", DataTypes.IntegerType)
              val secondHidden = createStructField("second", DataTypes.TimestampType)
              val schema = createStructType(
                keyField,
                firstHidden,
                secondHidden,
                createStructField("category", DataTypes.StringType)
              )

              assertFeatureDisabling(schema, Seq(firstHidden.name, secondHidden.name), FeatureAsserter.HIDDEN)
            }

            it("searchable") {

              val nonSearchableField = createStructField("description", DataTypes.StringType)
              val schema = createStructType(
                keyField,
                nonSearchableField,
                createStructField("date", DataTypes.DateType)
              )

              assertFeatureDisabling(schema, Seq(nonSearchableField.name), FeatureAsserter.SEARCHABLE)
            }

            it("sortable") {

              val nonSortableField = createStructField("level", DataTypes.IntegerType)
              val schema = createStructType(
                keyField,
                nonSortableField,
                createStructField(
                  "address",
                  createStructType(
                    createStructField("city", DataTypes.StringType)
                  )
                )
              )

              assertFeatureDisabling(schema, Seq(nonSortableField.name), FeatureAsserter.SORTABLE)
            }
          }

          describe("analyzers for") {

            // Define the schema
            lazy val topLevelField = "uuid"
            lazy val (parent, subField) = ("parent", "child")
            lazy val schemaForAnalyzerTests = createStructType(
              keyField,
              createStructField(topLevelField, DataTypes.StringType),
              createStructField(
                parent,
                createStructType(
                  createStructField(subField, DataTypes.StringType),
                  createStructField("other", DataTypes.TimestampType)
                )
              )
            )

            it("both searching and indexing") {

              // Create the index
              val analyzerType = SearchFieldAnalyzerType.SEARCH_AND_INDEX
              val searchFields = createIndexSettingAnalyzers(
                schemaForAnalyzerTests, analyzerType,
                Map(
                  "a1" -> Seq(topLevelField, s"$parent.$subField")
                )
              )

              // Asser that the analyzer has been set on both top-level field and sub-field
              analyzerType.getAnalyzerFrom(searchFields(topLevelField)) shouldBe analyzer
              val maybeSubField = maybeGetSubField(searchFields, parent, subField)
              maybeSubField shouldBe defined
              analyzerType.getAnalyzerFrom(maybeSubField.get) shouldBe analyzer
            }

            it("only searching") {

            }

            it("only indexing") {

            }
          }
        }
      }
    }
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
        safelyCreateIndex(previousSchema, Map.empty)
        indexExists(testIndex) shouldBe true
        assertMatchBetweenSchemaAndIndex(previousSchema, testIndex)

        // Trigger truncation and assert result
        val truncatingBuilder = new SearchWriteBuilder(
          WriteConfig(minimumOptionsForIndexCreation),
          currentSchema
        ).truncate()

        truncatingBuilder.build()
        indexExists(testIndex) shouldBe true
        assertMatchBetweenSchemaAndIndex(currentSchema, testIndex)
      }

      it("leave an existing index as-is if truncation flag is disabled") {

        indexExists(testIndex) shouldBe false
        safelyCreateIndex(previousSchema, Map.empty)
        indexExists(testIndex) shouldBe true
        assertMatchBetweenSchemaAndIndex(previousSchema, testIndex)

        // Trigger truncation and assert result
        val nonTruncatingBuilder = new SearchWriteBuilder(
          WriteConfig(minimumOptionsForIndexCreation),
          currentSchema,
          false
        )

        nonTruncatingBuilder.build()
        indexExists(testIndex) shouldBe true
        assertMatchBetweenSchemaAndIndex(previousSchema, testIndex)
      }
    }
  }
}
